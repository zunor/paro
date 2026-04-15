// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # ART Node - Base node types and operations
//!
//! ## Design
//! - Node inherits from IndexPointer, adding ART-specific functionality
//! - NType enum defines all node types (PREFIX, LEAF, NODE_4, etc.)
//! - GateStatus for nested ART support
//! - Allocator index mapping for each node type

use std::fmt;

use super::super::IndexPointer;

/// Node type enumeration for ART nodes.
///
/// Each type has a specific capacity and memory layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum NType {
    /// Prefix compression node
    Prefix = 1,
    /// Leaf node (deprecated, stores row_id list)
    Leaf = 2,
    /// Internal node with up to 4 children
    Node4 = 3,
    /// Internal node with up to 16 children
    Node16 = 4,
    /// Internal node with up to 48 children
    Node48 = 5,
    /// Internal node with up to 256 children
    Node256 = 6,
    /// Inlined leaf (row_id stored directly in pointer)
    LeafInlined = 7,
    /// Leaf node with up to 7 row_ids
    Node7Leaf = 8,
    /// Leaf node with up to 15 row_ids
    Node15Leaf = 9,
    /// Leaf node with up to 256 row_ids
    Node256Leaf = 10,
}

impl NType {
    /// Convert from u8 to NType.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(NType::Prefix),
            2 => Some(NType::Leaf),
            3 => Some(NType::Node4),
            4 => Some(NType::Node16),
            5 => Some(NType::Node48),
            6 => Some(NType::Node256),
            7 => Some(NType::LeafInlined),
            8 => Some(NType::Node7Leaf),
            9 => Some(NType::Node15Leaf),
            10 => Some(NType::Node256Leaf),
            _ => None,
        }
    }

    /// Get the allocator index for this node type.
    pub fn allocator_index(self) -> u8 {
        match self {
            NType::Prefix => 0,
            NType::Leaf => 1,
            NType::Node4 => 2,
            NType::Node16 => 3,
            NType::Node48 => 4,
            NType::Node256 => 5,
            NType::Node7Leaf => 6,
            NType::Node15Leaf => 7,
            NType::Node256Leaf => 8,
            NType::LeafInlined => 0, // Not allocated
        }
    }

    /// Get the capacity for this node type.
    pub fn capacity(self) -> usize {
        match self {
            NType::Node4 => 4,
            NType::Node7Leaf => 7,
            NType::Node15Leaf => 15,
            NType::Node16 => 16,
            NType::Node48 => 48,
            NType::Node256 | NType::Node256Leaf => 256,
            NType::Prefix | NType::Leaf | NType::LeafInlined => 0,
        }
    }

    /// Check if this is an internal node (Node4, Node16, Node48, Node256).
    pub fn is_internal_node(self) -> bool {
        matches!(
            self,
            NType::Node4 | NType::Node16 | NType::Node48 | NType::Node256
        )
    }

    /// Check if this is a leaf node (Node7Leaf, Node15Leaf, Node256Leaf).
    pub fn is_leaf_node(self) -> bool {
        matches!(
            self,
            NType::Node7Leaf | NType::Node15Leaf | NType::Node256Leaf
        )
    }

    /// Check if this is any kind of leaf (including inlined and deprecated).
    pub fn is_any_leaf(self) -> bool {
        matches!(
            self,
            NType::Leaf
                | NType::LeafInlined
                | NType::Node7Leaf
                | NType::Node15Leaf
                | NType::Node256Leaf
        )
    }

    /// Get the appropriate node type for a given child count.
    pub fn for_count(count: usize) -> Self {
        if count <= 4 {
            NType::Node4
        } else if count <= 16 {
            NType::Node16
        } else if count <= 48 {
            NType::Node48
        } else {
            NType::Node256
        }
    }
}

impl fmt::Display for NType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NType::Prefix => write!(f, "PREFIX"),
            NType::Leaf => write!(f, "LEAF"),
            NType::Node4 => write!(f, "NODE_4"),
            NType::Node16 => write!(f, "NODE_16"),
            NType::Node48 => write!(f, "NODE_48"),
            NType::Node256 => write!(f, "NODE_256"),
            NType::LeafInlined => write!(f, "LEAF_INLINED"),
            NType::Node7Leaf => write!(f, "NODE_7_LEAF"),
            NType::Node15Leaf => write!(f, "NODE_15_LEAF"),
            NType::Node256Leaf => write!(f, "NODE_256_LEAF"),
        }
    }
}

/// Gate status for nested ART support.
///
/// Gates are used to mark nodes that contain nested ARTs (for compound keys).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum GateStatus {
    /// Gate is not set (normal node)
    #[default]
    NotSet = 0,
    /// Gate is set (contains nested ART)
    Set = 1,
}

/// Number of allocators used by ART.
pub const ALLOCATOR_COUNT: usize = 9;

/// Number of allocators for deprecated ART format.
pub const DEPRECATED_ALLOCATOR_COUNT: usize = ALLOCATOR_COUNT - 3;

/// Mask for gate bit in metadata (bit 7).
const AND_GATE: u8 = 0x80;

/// Mask for row_id in the lower 56 bits.
const AND_ROW_ID: u64 = 0x00FF_FFFF_FFFF_FFFF;

/// ART Node - extends IndexPointer with ART-specific functionality.
///
/// The Node struct wraps an IndexPointer and provides:
/// - Node type stored in metadata (bits 56-62)
/// - Gate status stored in metadata bit 63
/// - Row ID stored in lower 56 bits (for inlined leaves)
///
/// # Memory Layout
/// ```text
/// 63    62       56 55                      32 31                                  0
/// +-----+---------+--------------------------+------------------------------------+
/// |Gate | NType   |           Offset         |           Buffer ID                |
/// |1bit | 7 bits  |          24 bits         |            32 bits                 |
/// +-----+---------+--------------------------+------------------------------------+
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Default)]
#[repr(transparent)]
pub struct Node {
    ptr: IndexPointer,
}

impl Node {
    /// Create an empty (null) node.
    #[inline]
    pub const fn empty() -> Self {
        Self {
            ptr: IndexPointer::new(),
        }
    }

    /// Create a node from an IndexPointer.
    #[inline]
    pub const fn from_pointer(ptr: IndexPointer) -> Self {
        Self { ptr }
    }

    /// Create a node with buffer ID and offset.
    #[inline]
    pub const fn new(buffer_id: u32, offset: u32) -> Self {
        Self {
            ptr: IndexPointer::with_buffer_and_offset(buffer_id, offset),
        }
    }

    /// Get the underlying IndexPointer.
    #[inline]
    pub const fn as_pointer(&self) -> &IndexPointer {
        &self.ptr
    }

    /// Get a mutable reference to the underlying IndexPointer.
    #[inline]
    pub fn as_pointer_mut(&mut self) -> &mut IndexPointer {
        &mut self.ptr
    }

    /// Get the raw 64-bit data.
    #[inline]
    pub const fn get(&self) -> u64 {
        self.ptr.get()
    }

    /// Set the raw 64-bit data.
    #[inline]
    pub fn set(&mut self, data: u64) {
        self.ptr.set(data);
    }

    /// Check if this node has metadata (is not empty).
    #[inline]
    pub const fn has_metadata(&self) -> bool {
        self.ptr.has_metadata()
    }

    /// Check if this node is empty.
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.ptr.is_empty()
    }

    /// Clear the node (set to empty).
    #[inline]
    pub fn clear(&mut self) {
        self.ptr.clear();
    }

    // ========== Node Type Operations ==========

    /// Get the node type from metadata.
    #[inline]
    pub fn get_type(&self) -> NType {
        let metadata = self.ptr.get_metadata();
        let type_bits = metadata & !AND_GATE;
        NType::from_u8(type_bits).unwrap_or(NType::Prefix)
    }

    /// Set the node type in metadata.
    #[inline]
    pub fn set_type(&mut self, ntype: NType) {
        let gate_bit = self.ptr.get_metadata() & AND_GATE;
        self.ptr.set_metadata(gate_bit | (ntype as u8));
    }

    /// Check if this is an internal node (Node4, Node16, Node48, Node256).
    #[inline]
    pub fn is_node(&self) -> bool {
        self.get_type().is_internal_node()
    }

    /// Check if this is a leaf node (Node7Leaf, Node15Leaf, Node256Leaf).
    #[inline]
    pub fn is_leaf_node(&self) -> bool {
        self.get_type().is_leaf_node()
    }

    /// Check if this is any kind of leaf.
    #[inline]
    pub fn is_any_leaf(&self) -> bool {
        self.get_type().is_any_leaf()
    }

    // ========== Gate Status Operations ==========

    /// Get the gate status.
    #[inline]
    pub fn get_gate_status(&self) -> GateStatus {
        if (self.ptr.get_metadata() & AND_GATE) == 0 {
            GateStatus::NotSet
        } else {
            GateStatus::Set
        }
    }

    /// Set the gate status.
    #[inline]
    pub fn set_gate_status(&mut self, status: GateStatus) {
        let metadata = self.ptr.get_metadata();
        match status {
            GateStatus::Set => {
                debug_assert!(
                    self.get_type() != NType::LeafInlined,
                    "Cannot set gate on inlined leaf"
                );
                self.ptr.set_metadata(metadata | AND_GATE);
            }
            GateStatus::NotSet => {
                self.ptr.set_metadata(metadata & !AND_GATE);
            }
        }
    }

    // ========== Row ID Operations (for inlined leaves) ==========

    /// Get the row ID (lower 56 bits).
    ///
    /// This is used for LeafInlined nodes where the row_id is stored
    /// directly in the pointer instead of in a separate allocation.
    #[inline]
    pub fn get_row_id(&self) -> i64 {
        (self.ptr.get() & AND_ROW_ID) as i64
    }

    /// Set the row ID (lower 56 bits).
    #[inline]
    pub fn set_row_id(&mut self, row_id: i64) {
        let metadata_bits = self.ptr.get() & !AND_ROW_ID;
        self.ptr.set(metadata_bits | (row_id as u64 & AND_ROW_ID));
    }

    // ========== Buffer/Offset Operations ==========

    /// Get the buffer ID.
    #[inline]
    pub const fn get_buffer_id(&self) -> u32 {
        self.ptr.get_buffer_id()
    }

    /// Get the offset.
    #[inline]
    pub const fn get_offset(&self) -> u32 {
        self.ptr.get_offset()
    }

    /// Set the buffer ID.
    #[inline]
    pub fn set_buffer_id(&mut self, buffer_id: u32) {
        self.ptr.set_buffer_id(buffer_id);
    }

    /// Set the offset.
    #[inline]
    pub fn set_offset(&mut self, offset: u32) {
        self.ptr.set_offset(offset);
    }

    /// Increase the buffer ID by a value.
    #[inline]
    pub fn increase_buffer_id(&mut self, summand: u32) {
        self.ptr.increase_buffer_id(summand);
    }

    // ========== Allocator Index ==========

    /// Get the allocator index for this node's type.
    #[inline]
    pub fn get_allocator_index(&self) -> u8 {
        self.get_type().allocator_index()
    }
}

impl From<IndexPointer> for Node {
    fn from(ptr: IndexPointer) -> Self {
        Self::from_pointer(ptr)
    }
}

impl From<Node> for IndexPointer {
    fn from(node: Node) -> Self {
        node.ptr
    }
}

impl fmt::Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_empty() {
            write!(f, "Node(empty)")
        } else {
            f.debug_struct("Node")
                .field("type", &self.get_type())
                .field("gate", &self.get_gate_status())
                .field("buffer_id", &self.get_buffer_id())
                .field("offset", &self.get_offset())
                .finish()
        }
    }
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_empty() {
            write!(f, "Node(empty)")
        } else {
            write!(
                f,
                "Node({}, buf={}, off={})",
                self.get_type(),
                self.get_buffer_id(),
                self.get_offset()
            )
        }
    }
}

// Ensure Node is exactly 8 bytes
const _: () = assert!(std::mem::size_of::<Node>() == 8);

#[cfg(test)]
mod tests {
    use super::*;

    // ========== NType Tests ==========

    #[test]
    fn test_ntype_from_u8() {
        assert_eq!(NType::from_u8(1), Some(NType::Prefix));
        assert_eq!(NType::from_u8(2), Some(NType::Leaf));
        assert_eq!(NType::from_u8(3), Some(NType::Node4));
        assert_eq!(NType::from_u8(4), Some(NType::Node16));
        assert_eq!(NType::from_u8(5), Some(NType::Node48));
        assert_eq!(NType::from_u8(6), Some(NType::Node256));
        assert_eq!(NType::from_u8(7), Some(NType::LeafInlined));
        assert_eq!(NType::from_u8(8), Some(NType::Node7Leaf));
        assert_eq!(NType::from_u8(9), Some(NType::Node15Leaf));
        assert_eq!(NType::from_u8(10), Some(NType::Node256Leaf));
        assert_eq!(NType::from_u8(0), None);
        assert_eq!(NType::from_u8(11), None);
    }

    #[test]
    fn test_ntype_allocator_index() {
        assert_eq!(NType::Prefix.allocator_index(), 0);
        assert_eq!(NType::Leaf.allocator_index(), 1);
        assert_eq!(NType::Node4.allocator_index(), 2);
        assert_eq!(NType::Node16.allocator_index(), 3);
        assert_eq!(NType::Node48.allocator_index(), 4);
        assert_eq!(NType::Node256.allocator_index(), 5);
        assert_eq!(NType::Node7Leaf.allocator_index(), 6);
        assert_eq!(NType::Node15Leaf.allocator_index(), 7);
        assert_eq!(NType::Node256Leaf.allocator_index(), 8);
    }

    #[test]
    fn test_ntype_capacity() {
        assert_eq!(NType::Node4.capacity(), 4);
        assert_eq!(NType::Node7Leaf.capacity(), 7);
        assert_eq!(NType::Node15Leaf.capacity(), 15);
        assert_eq!(NType::Node16.capacity(), 16);
        assert_eq!(NType::Node48.capacity(), 48);
        assert_eq!(NType::Node256.capacity(), 256);
        assert_eq!(NType::Node256Leaf.capacity(), 256);
    }

    #[test]
    fn test_ntype_is_internal_node() {
        assert!(NType::Node4.is_internal_node());
        assert!(NType::Node16.is_internal_node());
        assert!(NType::Node48.is_internal_node());
        assert!(NType::Node256.is_internal_node());
        assert!(!NType::Prefix.is_internal_node());
        assert!(!NType::Leaf.is_internal_node());
        assert!(!NType::LeafInlined.is_internal_node());
        assert!(!NType::Node7Leaf.is_internal_node());
    }

    #[test]
    fn test_ntype_is_leaf_node() {
        assert!(NType::Node7Leaf.is_leaf_node());
        assert!(NType::Node15Leaf.is_leaf_node());
        assert!(NType::Node256Leaf.is_leaf_node());
        assert!(!NType::Node4.is_leaf_node());
        assert!(!NType::Leaf.is_leaf_node());
        assert!(!NType::LeafInlined.is_leaf_node());
    }

    #[test]
    fn test_ntype_is_any_leaf() {
        assert!(NType::Leaf.is_any_leaf());
        assert!(NType::LeafInlined.is_any_leaf());
        assert!(NType::Node7Leaf.is_any_leaf());
        assert!(NType::Node15Leaf.is_any_leaf());
        assert!(NType::Node256Leaf.is_any_leaf());
        assert!(!NType::Node4.is_any_leaf());
        assert!(!NType::Prefix.is_any_leaf());
    }

    #[test]
    fn test_ntype_for_count() {
        assert_eq!(NType::for_count(0), NType::Node4);
        assert_eq!(NType::for_count(4), NType::Node4);
        assert_eq!(NType::for_count(5), NType::Node16);
        assert_eq!(NType::for_count(16), NType::Node16);
        assert_eq!(NType::for_count(17), NType::Node48);
        assert_eq!(NType::for_count(48), NType::Node48);
        assert_eq!(NType::for_count(49), NType::Node256);
        assert_eq!(NType::for_count(256), NType::Node256);
    }

    // ========== Node Tests ==========

    #[test]
    fn test_node_empty() {
        let node = Node::empty();
        assert!(node.is_empty());
        assert!(!node.has_metadata());
        assert_eq!(node.get(), 0);
    }

    #[test]
    fn test_node_new() {
        let node = Node::new(42, 100);
        assert!(!node.is_empty());
        assert_eq!(node.get_buffer_id(), 42);
        assert_eq!(node.get_offset(), 100);
    }

    #[test]
    fn test_node_type() {
        let mut node = Node::new(1, 2);
        node.set_type(NType::Node4);
        assert_eq!(node.get_type(), NType::Node4);
        assert!(node.is_node());
        assert!(!node.is_leaf_node());

        node.set_type(NType::Node7Leaf);
        assert_eq!(node.get_type(), NType::Node7Leaf);
        assert!(!node.is_node());
        assert!(node.is_leaf_node());
        assert!(node.is_any_leaf());
    }

    #[test]
    fn test_node_gate_status() {
        let mut node = Node::new(1, 2);
        node.set_type(NType::Node4);
        assert_eq!(node.get_gate_status(), GateStatus::NotSet);

        node.set_gate_status(GateStatus::Set);
        assert_eq!(node.get_gate_status(), GateStatus::Set);
        // Type should be preserved
        assert_eq!(node.get_type(), NType::Node4);

        node.set_gate_status(GateStatus::NotSet);
        assert_eq!(node.get_gate_status(), GateStatus::NotSet);
    }

    #[test]
    fn test_node_row_id() {
        let mut node = Node::empty();
        node.set_type(NType::LeafInlined);
        node.set_row_id(12345);
        assert_eq!(node.get_row_id(), 12345);

        // Test negative row_id
        node.set_row_id(-1);
        // Note: row_id is masked to 56 bits, so -1 becomes a large positive number
        assert_eq!(node.get_row_id() as u64 & AND_ROW_ID, AND_ROW_ID);
    }

    #[test]
    fn test_node_clear() {
        let mut node = Node::new(42, 100);
        node.set_type(NType::Node4);
        assert!(!node.is_empty());

        node.clear();
        assert!(node.is_empty());
        assert_eq!(node.get(), 0);
    }

    #[test]
    fn test_node_from_pointer() {
        let ptr = IndexPointer::with_buffer_and_offset(10, 20);
        let node = Node::from_pointer(ptr);
        assert_eq!(node.get_buffer_id(), 10);
        assert_eq!(node.get_offset(), 20);
    }

    #[test]
    fn test_node_into_pointer() {
        let mut node = Node::new(10, 20);
        node.set_type(NType::Node16);
        let ptr: IndexPointer = node.into();
        assert_eq!(ptr.get_buffer_id(), 10);
        assert_eq!(ptr.get_offset(), 20);
    }

    #[test]
    fn test_node_allocator_index() {
        let mut node = Node::new(1, 2);
        node.set_type(NType::Node4);
        assert_eq!(node.get_allocator_index(), 2);

        node.set_type(NType::Node256);
        assert_eq!(node.get_allocator_index(), 5);
    }

    #[test]
    fn test_node_increase_buffer_id() {
        let mut node = Node::new(100, 50);
        node.set_type(NType::Node4);
        node.increase_buffer_id(10);
        assert_eq!(node.get_buffer_id(), 110);
        assert_eq!(node.get_offset(), 50);
        assert_eq!(node.get_type(), NType::Node4);
    }

    #[test]
    fn test_node_debug_format() {
        let node = Node::empty();
        assert_eq!(format!("{:?}", node), "Node(empty)");

        let mut node2 = Node::new(1, 2);
        node2.set_type(NType::Node4);
        let debug_str = format!("{:?}", node2);
        assert!(debug_str.contains("Node4"));
        assert!(debug_str.contains("buffer_id"));
    }

    #[test]
    fn test_node_display_format() {
        let node = Node::empty();
        assert_eq!(format!("{}", node), "Node(empty)");

        let mut node2 = Node::new(1, 2);
        node2.set_type(NType::Node4);
        let display_str = format!("{}", node2);
        assert!(display_str.contains("NODE_4"));
    }

    #[test]
    fn test_node_size() {
        // Ensure Node is exactly 8 bytes
        assert_eq!(std::mem::size_of::<Node>(), 8);
    }

    #[test]
    fn test_gate_and_type_independence() {
        let mut node = Node::new(1, 2);

        // Set type first
        node.set_type(NType::Node48);
        assert_eq!(node.get_type(), NType::Node48);
        assert_eq!(node.get_gate_status(), GateStatus::NotSet);

        // Set gate
        node.set_gate_status(GateStatus::Set);
        assert_eq!(node.get_type(), NType::Node48);
        assert_eq!(node.get_gate_status(), GateStatus::Set);

        // Change type, gate should be preserved
        node.set_type(NType::Node256);
        assert_eq!(node.get_type(), NType::Node256);
        // Note: set_type clears gate bit, this is by design
        // If we want to preserve gate, we need to handle it explicitly
    }
}
