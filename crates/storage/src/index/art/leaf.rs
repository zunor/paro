//! # ART Leaf Nodes - Leaf node types for storing row IDs
//!
//! ## Design
//!
//! There are three types of leaves:
//! 1. LEAF_INLINED: Inlines a row ID in a Node pointer (no allocation needed)
//! 2. LEAF: Deprecated. A linked list of Leaf nodes containing row IDs
//! 3. Nested leaves indicated by gate nodes. If an ART key contains multiple row IDs,
//!    then we use the row IDs as keys and create a nested ART behind the gate node.
//!
//! For nested leaves, we have three node types:
//! - Node7Leaf: Holds up to 7 sorted bytes
//! - Node15Leaf: Holds up to 15 sorted bytes
//! - Node256Leaf: A bitmask containing 256 bits

use std::collections::BTreeSet;

use super::internal_node::Node4;
use super::node::{GateStatus, NType, Node};
use super::prefix::{Prefix, ROW_ID_COUNT, ROW_ID_SIZE};
use super::ARTKey;
use crate::index::fixed_size_allocator::FixedSizeAllocator;
use paro_common::allocator::ArenaAllocator;

/// Maximum row ID that can be stored locally (56 bits).
pub const MAX_ROW_ID_LOCAL: i64 = 0x00FF_FFFF_FFFF_FFFF;

/// Deprecated leaf size (number of row IDs per leaf node).
pub const DEPRECATED_LEAF_SIZE: usize = 4;

/// Mask for extracting the last byte from a row ID.
pub const AND_LAST_BYTE: u64 = 0xFFFF_FFFF_FFFF_FF00;

/// Leaf node operations for ART index.
///
/// This struct provides static methods for working with leaf nodes.
/// Leaf nodes store row IDs and come in several forms:
/// - Inlined: row ID stored directly in the Node pointer
/// - Nested: row IDs stored as keys in a nested ART
/// - Deprecated: linked list of leaf nodes (for backward compatibility)
pub struct Leaf;

impl Leaf {
    /// Create a new inlined leaf node.
    ///
    /// The row ID is stored directly in the node pointer, avoiding allocation.
    ///
    /// # Arguments
    /// * `node` - Output node to initialize
    /// * `row_id` - The row ID to store
    #[inline]
    pub fn new(node: &mut Node, row_id: i64) {
        debug_assert!(
            row_id < MAX_ROW_ID_LOCAL,
            "Row ID exceeds maximum for local storage"
        );
        node.clear();
        node.set_type(NType::LeafInlined);
        node.set_row_id(row_id);
    }

    /// Merge two inlined leaf nodes into a nested structure.
    ///
    /// When two row IDs need to be stored at the same key position,
    /// we create a nested ART using the row IDs as keys.
    ///
    /// # Arguments
    /// * `arena` - Arena allocator for temporary allocations
    /// * `allocators` - Array of allocators for all node types
    /// * `left` - First inlined leaf (will be replaced with merged result)
    /// * `right` - Second inlined leaf
    /// * `status` - Current gate status
    /// * `depth` - Current depth in the tree
    /// * `prefix_count` - Maximum prefix bytes per node
    pub fn merge_inlined(
        arena: &mut ArenaAllocator,
        allocators: &mut [FixedSizeAllocator],
        left: &mut Node,
        right: &Node,
        status: GateStatus,
        mut depth: usize,
        prefix_count: usize,
    ) {
        const PREFIX_ALLOC: usize = 0;
        const NODE4_ALLOC: usize = 2;
        const NODE7_LEAF_ALLOC: usize = 6;

        debug_assert_eq!(left.get_type(), NType::LeafInlined);
        debug_assert_eq!(right.get_type(), NType::LeafInlined);

        // Toggle gate status
        let new_status = if status == GateStatus::NotSet {
            GateStatus::Set
        } else {
            GateStatus::NotSet
        };

        if new_status == GateStatus::Set {
            // Case 1: We are outside a nested leaf, create a nested leaf
            depth = 0;
        }
        // Otherwise, case 2: we are in a nested leaf with two 'compressed' prefixes

        // Get the corresponding row IDs and their ART keys
        let left_row_id = left.get_row_id();
        let right_row_id = right.get_row_id();

        let left_key = ARTKey::from_i64(arena, left_row_id).expect("Failed to create left key");
        let right_key = ARTKey::from_i64(arena, right_row_id).expect("Failed to create right key");

        let pos = left_key.get_mismatch_pos(&right_key, depth);

        left.clear();

        // Determine where to create the node (at the end of prefix chain or directly)
        let node_ptr: *mut Node = if pos != depth {
            // The row IDs share a prefix
            Prefix::create_chain(
                &mut allocators[PREFIX_ALLOC],
                left,
                prefix_count,
                &left_key,
                depth,
                pos - depth,
            )
        } else {
            left as *mut Node
        };
        let gate_node: *mut Node = if new_status == GateStatus::Set {
            left as *mut Node
        } else {
            node_ptr
        };

        let left_byte = left_key.get(pos);
        let right_byte = right_key.get(pos);

        if pos == ROW_ID_COUNT as usize {
            // The row IDs differ on the last byte
            unsafe {
                Node7Leaf::new(&mut allocators[NODE7_LEAF_ALLOC], &mut *node_ptr);
                Node7Leaf::insert_byte_internal(
                    &mut allocators[NODE7_LEAF_ALLOC],
                    &mut *node_ptr,
                    left_byte,
                );
                Node7Leaf::insert_byte_internal(
                    &mut allocators[NODE7_LEAF_ALLOC],
                    &mut *node_ptr,
                    right_byte,
                );
                (*gate_node).set_gate_status(new_status);
            }
            return;
        }

        // Create and insert the (compressed) children
        // We inline directly into the node, instead of creating prefixes
        // with a single inlined leaf as their child
        unsafe {
            Node4::new(&mut allocators[NODE4_ALLOC], &mut *node_ptr);

            let mut left_child = Node::empty();
            Leaf::new(&mut left_child, left_row_id);
            {
                let mut handle = Node4::get_mut(&allocators[NODE4_ALLOC], *node_ptr);
                handle.insert_child_internal(left_byte, left_child);
            }

            let mut right_child = Node::empty();
            Leaf::new(&mut right_child, right_row_id);
            {
                let mut handle = Node4::get_mut(&allocators[NODE4_ALLOC], *node_ptr);
                handle.insert_child_internal(right_byte, right_child);
            }

            (*gate_node).set_gate_status(new_status);
        }
    }

    // ========== Deprecated Leaf Operations ==========

    /// Free a linked list of deprecated leaf nodes.
    pub fn deprecated_free(allocator: &mut FixedSizeAllocator, node: &mut Node) {
        debug_assert_eq!(node.get_type(), NType::Leaf);

        let mut current = *node;
        while current.has_metadata() {
            let leaf = DeprecatedLeaf::get(allocator, current);
            let next = leaf.get_next();
            allocator.free(current.into());
            current = next;
        }
        node.clear();
    }

    /// Get all row IDs from a deprecated leaf linked list.
    ///
    /// # Returns
    /// `true` if all row IDs were collected, `false` if max_count was exceeded.
    pub fn deprecated_get_row_ids(
        allocator: &FixedSizeAllocator,
        node: &Node,
        row_ids: &mut BTreeSet<i64>,
        max_count: usize,
    ) -> bool {
        debug_assert_eq!(node.get_type(), NType::Leaf);

        let mut current = *node;
        while current.has_metadata() {
            let leaf = DeprecatedLeaf::get(allocator, current);
            let count = leaf.get_count();

            if row_ids.len() + count as usize > max_count {
                return false;
            }

            for i in 0..count {
                row_ids.insert(leaf.get_row_id(i));
            }

            current = leaf.get_next();
        }
        true
    }

    /// Vacuum a deprecated leaf linked list.
    pub fn deprecated_vacuum(allocator: &mut FixedSizeAllocator, node: &mut Node) {
        debug_assert!(node.has_metadata());
        debug_assert_eq!(node.get_type(), NType::Leaf);

        let mut current = *node;
        while current.has_metadata() {
            if allocator.needs_vacuum(current.into()) {
                let new_ptr = allocator.vacuum_pointer(current.into());
                current = Node::from_pointer(new_ptr);
                current.set_type(NType::Leaf);
            }

            let leaf = DeprecatedLeaf::get(allocator, current);
            current = leaf.get_next();
        }
    }

    /// Transform a deprecated leaf to a nested structure.
    ///
    /// This is called when we encounter a deprecated LEAF node during insert.
    /// The deprecated leaf is converted to the new nested leaf format.
    ///
    /// # Arguments
    /// * `art` - The ART index (for allocators)
    /// * `node` - The deprecated leaf node to transform
    pub fn transform_to_nested<T>(_art: &mut T, node: &mut Node) {
        // For now, this is a placeholder that just clears the node.
        // In a full implementation, we would:
        // 1. Read all row IDs from the deprecated leaf chain
        // 2. Create a nested ART structure with those row IDs
        // 3. Free the deprecated leaf nodes
        //
        // Since deprecated leaves are for backward compatibility and
        // new inserts use the nested format, we can defer full implementation.
        debug_assert_eq!(node.get_type(), NType::Leaf);
        // TODO: Implement full transformation
        // For now, panic to indicate this code path needs implementation
        panic!("transform_to_nested not yet implemented for deprecated leaves");
    }
}

/// Deprecated leaf node structure.
///
/// This is used for backward compatibility with older ART formats.
/// Each node stores up to DEPRECATED_LEAF_SIZE row IDs and a pointer to the next node.
///
/// Memory layout:
/// ```text
/// +-------+------------------+------+
/// | count | row_ids[4]       | next |
/// | (1B)  | (4 * 8B = 32B)   | (8B) |
/// +-------+------------------+------+
/// ```
pub struct DeprecatedLeaf {
    data: *mut u8,
}

impl DeprecatedLeaf {
    /// Size of a deprecated leaf node in bytes.
    pub const SIZE: usize = 1 + DEPRECATED_LEAF_SIZE * ROW_ID_SIZE + std::mem::size_of::<Node>();

    /// Get a deprecated leaf handle.
    pub fn get(allocator: &FixedSizeAllocator, node: Node) -> Self {
        debug_assert_eq!(node.get_type(), NType::Leaf);
        let data = allocator.get(node.into(), false);
        Self { data }
    }

    /// Get a mutable deprecated leaf handle.
    pub fn get_mut(allocator: &FixedSizeAllocator, node: Node) -> Self {
        debug_assert_eq!(node.get_type(), NType::Leaf);
        let data = allocator.get(node.into(), true);
        Self { data }
    }

    /// Get the count of row IDs in this node.
    #[inline]
    pub fn get_count(&self) -> u8 {
        unsafe { *self.data }
    }

    /// Set the count of row IDs.
    #[inline]
    pub fn set_count(&mut self, count: u8) {
        unsafe { *self.data = count };
    }

    /// Get a row ID at the given index.
    #[inline]
    pub fn get_row_id(&self, index: u8) -> i64 {
        debug_assert!((index as usize) < DEPRECATED_LEAF_SIZE);
        unsafe {
            let ptr = self.data.add(1 + index as usize * ROW_ID_SIZE) as *const i64;
            *ptr
        }
    }

    /// Set a row ID at the given index.
    #[inline]
    pub fn set_row_id(&mut self, index: u8, row_id: i64) {
        debug_assert!((index as usize) < DEPRECATED_LEAF_SIZE);
        unsafe {
            let ptr = self.data.add(1 + index as usize * ROW_ID_SIZE) as *mut i64;
            *ptr = row_id;
        }
    }

    /// Get the next node pointer.
    #[inline]
    pub fn get_next(&self) -> Node {
        unsafe {
            let ptr = self.data.add(1 + DEPRECATED_LEAF_SIZE * ROW_ID_SIZE) as *const Node;
            *ptr
        }
    }

    /// Get a mutable reference to the next node pointer.
    #[inline]
    pub fn get_next_mut(&mut self) -> &mut Node {
        unsafe {
            let ptr = self.data.add(1 + DEPRECATED_LEAF_SIZE * ROW_ID_SIZE) as *mut Node;
            &mut *ptr
        }
    }

    /// Set the next node pointer.
    #[inline]
    pub fn set_next(&mut self, next: Node) {
        unsafe {
            let ptr = self.data.add(1 + DEPRECATED_LEAF_SIZE * ROW_ID_SIZE) as *mut Node;
            *ptr = next;
        }
    }
}

// SAFETY: DeprecatedLeaf is just a pointer to allocator-managed memory
unsafe impl Send for DeprecatedLeaf {}
unsafe impl Sync for DeprecatedLeaf {}

/// Node7Leaf holds up to 7 sorted bytes.
///
/// Memory layout:
/// ```text
/// +-------+----------+
/// | count | key[7]   |
/// | (1B)  | (7B)     |
/// +-------+----------+
/// ```
pub struct Node7Leaf {
    data: *mut u8,
}

impl Node7Leaf {
    /// Capacity of Node7Leaf.
    pub const CAPACITY: usize = 7;

    /// Size of Node7Leaf in bytes.
    pub const SIZE: usize = 1 + Self::CAPACITY;

    /// Create a new Node7Leaf.
    pub fn new(allocator: &mut FixedSizeAllocator, node: &mut Node) {
        let ptr = allocator.new_segment();
        *node = Node::from_pointer(ptr);
        node.set_type(NType::Node7Leaf);

        let handle = Self::get_mut(allocator, *node);
        handle.set_count(0);
    }

    /// Get a Node7Leaf handle.
    pub fn get(allocator: &FixedSizeAllocator, node: Node) -> Self {
        debug_assert_eq!(node.get_type(), NType::Node7Leaf);
        let data = allocator.get(node.into(), false);
        Self { data }
    }

    /// Get a mutable Node7Leaf handle.
    pub fn get_mut(allocator: &FixedSizeAllocator, node: Node) -> Self {
        debug_assert_eq!(node.get_type(), NType::Node7Leaf);
        let data = allocator.get(node.into(), true);
        Self { data }
    }

    /// Get the count of bytes.
    #[inline]
    pub fn get_count(&self) -> u8 {
        unsafe { *self.data }
    }

    /// Set the count of bytes.
    #[inline]
    fn set_count(&self, count: u8) {
        unsafe { *self.data = count };
    }

    /// Get a byte at the given index.
    #[inline]
    pub fn get_byte(&self, index: u8) -> u8 {
        debug_assert!((index as usize) < Self::CAPACITY);
        unsafe { *self.data.add(1 + index as usize) }
    }

    /// Set a byte at the given index.
    #[inline]
    fn set_byte(&self, index: u8, byte: u8) {
        debug_assert!((index as usize) < Self::CAPACITY);
        unsafe { *self.data.add(1 + index as usize) = byte };
    }

    /// Check if a byte exists in this leaf.
    pub fn has_byte(&self, byte: u8) -> bool {
        let count = self.get_count();
        for i in 0..count {
            if self.get_byte(i) == byte {
                return true;
            }
        }
        false
    }

    /// Get all bytes as a slice.
    pub fn get_bytes(&self) -> &[u8] {
        let count = self.get_count() as usize;
        unsafe { std::slice::from_raw_parts(self.data.add(1), count) }
    }

    /// Get the first byte >= the given byte.
    ///
    /// # Returns
    /// `Some(byte)` if found, `None` otherwise.
    pub fn get_next_byte(&self, byte: u8) -> Option<u8> {
        let count = self.get_count();
        for i in 0..count {
            let b = self.get_byte(i);
            if b >= byte {
                return Some(b);
            }
        }
        None
    }

    /// Insert a byte (internal, assumes space is available).
    pub fn insert_byte_internal(allocator: &mut FixedSizeAllocator, node: &mut Node, byte: u8) {
        let handle = Self::get_mut(allocator, *node);
        let count = handle.get_count();

        // Find insertion position (maintain sorted order)
        let mut pos = 0u8;
        while pos < count && handle.get_byte(pos) < byte {
            pos += 1;
        }

        // Shift bytes to make room
        for i in (pos..count).rev() {
            let b = handle.get_byte(i);
            handle.set_byte(i + 1, b);
        }

        handle.set_byte(pos, byte);
        handle.set_count(count + 1);
    }

    /// Insert a byte, growing to Node15Leaf if necessary.
    pub fn insert_byte(
        node7_allocator: &mut FixedSizeAllocator,
        node15_allocator: &mut FixedSizeAllocator,
        node: &mut Node,
        byte: u8,
    ) {
        let handle = Self::get(node7_allocator, *node);
        let count = handle.get_count() as usize;

        if count < Self::CAPACITY {
            Self::insert_byte_internal(node7_allocator, node, byte);
            return;
        }

        // Node is full, grow to Node15Leaf
        let node7 = *node;
        Node15Leaf::grow_from_node7(node7_allocator, node15_allocator, node, node7);
        Node15Leaf::insert_byte(node15_allocator, node, byte);
    }

    /// Delete a byte, handling compression.
    ///
    /// # Arguments
    /// * `node7_allocator` - Allocator for Node7Leaf
    /// * `node` - The node to delete from
    /// * `prefix` - Parent prefix node (for compression)
    /// * `byte` - The byte to delete
    /// * `row_id_key` - The row ID key (for compression)
    /// * `prefix_count` - Maximum prefix bytes
    pub fn delete_byte(
        node7_allocator: &mut FixedSizeAllocator,
        prefix_allocator: &mut FixedSizeAllocator,
        node: &mut Node,
        prefix: &mut Node,
        byte: u8,
        row_id_key: &ARTKey,
        prefix_count: usize,
    ) {
        let handle = Self::get_mut(node7_allocator, *node);
        let count = handle.get_count();

        // Find and remove the byte
        let mut pos = 0u8;
        while pos < count && handle.get_byte(pos) != byte {
            pos += 1;
        }

        // Shift remaining bytes
        for i in pos..(count - 1) {
            let b = handle.get_byte(i + 1);
            handle.set_byte(i, b);
        }
        handle.set_count(count - 1);

        if count - 1 != 1 {
            return;
        }

        // Compress one-way nodes
        debug_assert_eq!(node.get_gate_status(), GateStatus::NotSet);

        // Get the remaining row ID
        let remaining_byte = handle.get_byte(0);
        let remainder = (row_id_key.get_row_id() as u64 & AND_LAST_BYTE) | remaining_byte as u64;

        // Free the prefix (nodes) and inline the remainder
        if prefix.get_type() == NType::Prefix {
            // Free the prefix chain
            let mut current = *prefix;
            while current.get_type() == NType::Prefix {
                let p = Prefix::new(prefix_allocator, current, prefix_count, false);
                let next = p.get_child();
                Prefix::free_node(prefix_allocator, current);
                current = next;
            }
            Leaf::new(prefix, remainder as i64);
            return;
        }

        // Free the Node7Leaf and inline the remainder
        node7_allocator.free((*node).into());
        Leaf::new(node, remainder as i64);
    }

    /// Shrink a Node15Leaf to Node7Leaf.
    pub fn shrink_from_node15(
        node7_allocator: &mut FixedSizeAllocator,
        node15_allocator: &mut FixedSizeAllocator,
        node7: &mut Node,
        node15: Node,
    ) {
        Self::new(node7_allocator, node7);
        node7.set_gate_status(node15.get_gate_status());

        let n7 = Self::get_mut(node7_allocator, *node7);
        let n15 = Node15Leaf::get(node15_allocator, node15);

        let count = n15.get_count();
        n7.set_count(count);
        for i in 0..count {
            n7.set_byte(i, n15.get_byte(i));
        }

        node15_allocator.free(node15.into());
    }
}

// SAFETY: Node7Leaf is just a pointer to allocator-managed memory
unsafe impl Send for Node7Leaf {}
unsafe impl Sync for Node7Leaf {}

/// Node15Leaf holds up to 15 sorted bytes.
///
/// Memory layout:
/// ```text
/// +-------+----------+
/// | count | key[15]  |
/// | (1B)  | (15B)    |
/// +-------+----------+
/// ```
pub struct Node15Leaf {
    data: *mut u8,
}

impl Node15Leaf {
    /// Capacity of Node15Leaf.
    pub const CAPACITY: usize = 15;

    /// Size of Node15Leaf in bytes.
    pub const SIZE: usize = 1 + Self::CAPACITY;

    /// Create a new Node15Leaf.
    pub fn new(allocator: &mut FixedSizeAllocator, node: &mut Node) {
        let ptr = allocator.new_segment();
        *node = Node::from_pointer(ptr);
        node.set_type(NType::Node15Leaf);

        let handle = Self::get_mut(allocator, *node);
        handle.set_count(0);
    }

    /// Get a Node15Leaf handle.
    pub fn get(allocator: &FixedSizeAllocator, node: Node) -> Self {
        debug_assert_eq!(node.get_type(), NType::Node15Leaf);
        let data = allocator.get(node.into(), false);
        Self { data }
    }

    /// Get a mutable Node15Leaf handle.
    pub fn get_mut(allocator: &FixedSizeAllocator, node: Node) -> Self {
        debug_assert_eq!(node.get_type(), NType::Node15Leaf);
        let data = allocator.get(node.into(), true);
        Self { data }
    }

    /// Get the count of bytes.
    #[inline]
    pub fn get_count(&self) -> u8 {
        unsafe { *self.data }
    }

    /// Set the count of bytes.
    #[inline]
    fn set_count(&self, count: u8) {
        unsafe { *self.data = count };
    }

    /// Get a byte at the given index.
    #[inline]
    pub fn get_byte(&self, index: u8) -> u8 {
        debug_assert!((index as usize) < Self::CAPACITY);
        unsafe { *self.data.add(1 + index as usize) }
    }

    /// Set a byte at the given index.
    #[inline]
    fn set_byte(&self, index: u8, byte: u8) {
        debug_assert!((index as usize) < Self::CAPACITY);
        unsafe { *self.data.add(1 + index as usize) = byte };
    }

    /// Check if a byte exists in this leaf.
    pub fn has_byte(&self, byte: u8) -> bool {
        let count = self.get_count();
        for i in 0..count {
            if self.get_byte(i) == byte {
                return true;
            }
        }
        false
    }

    /// Get all bytes as a slice.
    pub fn get_bytes(&self) -> &[u8] {
        let count = self.get_count() as usize;
        unsafe { std::slice::from_raw_parts(self.data.add(1), count) }
    }

    /// Get the first byte >= the given byte.
    pub fn get_next_byte(&self, byte: u8) -> Option<u8> {
        let count = self.get_count();
        for i in 0..count {
            let b = self.get_byte(i);
            if b >= byte {
                return Some(b);
            }
        }
        None
    }

    /// Insert a byte (internal, assumes space is available).
    pub fn insert_byte_internal(allocator: &mut FixedSizeAllocator, node: &mut Node, byte: u8) {
        let handle = Self::get_mut(allocator, *node);
        let count = handle.get_count();

        // Find insertion position (maintain sorted order)
        let mut pos = 0u8;
        while pos < count && handle.get_byte(pos) < byte {
            pos += 1;
        }

        // Shift bytes to make room
        for i in (pos..count).rev() {
            let b = handle.get_byte(i);
            handle.set_byte(i + 1, b);
        }

        handle.set_byte(pos, byte);
        handle.set_count(count + 1);
    }

    /// Insert a byte, growing to Node256Leaf if necessary.
    pub fn insert_byte(allocator: &mut FixedSizeAllocator, node: &mut Node, byte: u8) {
        let handle = Self::get(allocator, *node);
        let count = handle.get_count() as usize;

        if count < Self::CAPACITY {
            Self::insert_byte_internal(allocator, node, byte);
            return;
        }

        // Node is full, grow to Node256Leaf
        // Note: Node256Leaf uses a different allocator, handled by caller
        panic!("Node15Leaf is full, should grow to Node256Leaf");
    }

    /// Insert a byte with growth support.
    pub fn insert_byte_with_growth(
        node15_allocator: &mut FixedSizeAllocator,
        node256_allocator: &mut FixedSizeAllocator,
        node: &mut Node,
        byte: u8,
    ) {
        let handle = Self::get(node15_allocator, *node);
        let count = handle.get_count() as usize;

        if count < Self::CAPACITY {
            Self::insert_byte_internal(node15_allocator, node, byte);
            return;
        }

        // Node is full, grow to Node256Leaf
        let node15 = *node;
        Node256Leaf::grow_from_node15(node15_allocator, node256_allocator, node, node15);
        Node256Leaf::insert_byte(node256_allocator, node, byte);
    }

    /// Delete a byte, shrinking to Node7Leaf if necessary.
    pub fn delete_byte(
        node7_allocator: &mut FixedSizeAllocator,
        node15_allocator: &mut FixedSizeAllocator,
        node: &mut Node,
        byte: u8,
    ) {
        let handle = Self::get_mut(node15_allocator, *node);
        let count = handle.get_count();

        // Find and remove the byte
        let mut pos = 0u8;
        while pos < count && handle.get_byte(pos) != byte {
            pos += 1;
        }

        // Shift remaining bytes
        for i in pos..(count - 1) {
            let b = handle.get_byte(i + 1);
            handle.set_byte(i, b);
        }
        handle.set_count(count - 1);

        if count > Node7Leaf::CAPACITY as u8 {
            return;
        }

        // Shrink to Node7Leaf
        let node15 = *node;
        Node7Leaf::shrink_from_node15(node7_allocator, node15_allocator, node, node15);
    }

    /// Grow from Node7Leaf.
    pub fn grow_from_node7(
        node7_allocator: &mut FixedSizeAllocator,
        node15_allocator: &mut FixedSizeAllocator,
        node15: &mut Node,
        node7: Node,
    ) {
        let n7 = Node7Leaf::get(node7_allocator, node7);

        Self::new(node15_allocator, node15);
        node15.set_gate_status(node7.get_gate_status());

        let n15 = Self::get_mut(node15_allocator, *node15);
        let count = n7.get_count();
        n15.set_count(count);
        for i in 0..count {
            n15.set_byte(i, n7.get_byte(i));
        }

        node7_allocator.free(node7.into());
    }

    /// Shrink from Node256Leaf.
    pub fn shrink_from_node256(
        node15_allocator: &mut FixedSizeAllocator,
        node256_allocator: &mut FixedSizeAllocator,
        node15: &mut Node,
        node256: Node,
    ) {
        Self::new(node15_allocator, node15);
        node15.set_gate_status(node256.get_gate_status());

        let n15 = Self::get_mut(node15_allocator, *node15);
        let n256 = Node256Leaf::get(node256_allocator, node256);

        // Copy all set bits from Node256Leaf
        let mut count = 0u8;
        for i in 0u16..256 {
            if n256.has_byte(i as u8) {
                n15.set_byte(count, i as u8);
                count += 1;
            }
        }
        n15.set_count(count);

        node256_allocator.free(node256.into());
    }
}

// SAFETY: Node15Leaf is just a pointer to allocator-managed memory
unsafe impl Send for Node15Leaf {}
unsafe impl Sync for Node15Leaf {}

/// Node256Leaf is a bitmask containing 256 bits.
///
/// Memory layout:
/// ```text
/// +-------+------------------+
/// | count | mask[32]         |
/// | (2B)  | (32B)            |
/// +-------+------------------+
/// ```
pub struct Node256Leaf {
    data: *mut u8,
}

impl Node256Leaf {
    /// Capacity of Node256Leaf.
    pub const CAPACITY: usize = 256;

    /// Size of Node256Leaf in bytes (2 bytes count + 32 bytes mask).
    pub const SIZE: usize = 2 + 32;

    /// Shrink threshold (same as Node48).
    pub const SHRINK_THRESHOLD: usize = 12;

    /// Create a new Node256Leaf.
    pub fn new(allocator: &mut FixedSizeAllocator, node: &mut Node) {
        let ptr = allocator.new_segment();
        *node = Node::from_pointer(ptr);
        node.set_type(NType::Node256Leaf);

        let handle = Self::get_mut(allocator, *node);
        handle.set_count(0);
        // Clear the mask
        handle.clear_mask();
    }

    /// Get a Node256Leaf handle.
    pub fn get(allocator: &FixedSizeAllocator, node: Node) -> Self {
        debug_assert_eq!(node.get_type(), NType::Node256Leaf);
        let data = allocator.get(node.into(), false);
        Self { data }
    }

    /// Get a mutable Node256Leaf handle.
    pub fn get_mut(allocator: &FixedSizeAllocator, node: Node) -> Self {
        debug_assert_eq!(node.get_type(), NType::Node256Leaf);
        let data = allocator.get(node.into(), true);
        Self { data }
    }

    /// Get the count of bytes.
    #[inline]
    pub fn get_count(&self) -> u16 {
        // Read as two bytes to avoid alignment issues
        unsafe {
            let lo = *self.data as u16;
            let hi = (*self.data.add(1) as u16) << 8;
            lo | hi
        }
    }

    /// Set the count of bytes.
    #[inline]
    fn set_count(&self, count: u16) {
        // Write as two bytes to avoid alignment issues
        unsafe {
            *self.data = count as u8;
            *self.data.add(1) = (count >> 8) as u8;
        }
    }

    /// Get the mask byte at the given index.
    #[inline]
    fn get_mask_byte(&self, index: usize) -> u8 {
        debug_assert!(index < 32);
        unsafe { *self.data.add(2 + index) }
    }

    /// Set the mask byte at the given index.
    #[inline]
    fn set_mask_byte(&self, index: usize, value: u8) {
        debug_assert!(index < 32);
        unsafe { *self.data.add(2 + index) = value };
    }

    /// Clear the mask.
    fn clear_mask(&self) {
        for i in 0..32 {
            self.set_mask_byte(i, 0);
        }
    }

    /// Check if a byte is set in the mask.
    #[inline]
    pub fn has_byte(&self, byte: u8) -> bool {
        let byte_idx = byte as usize / 8;
        let bit_idx = byte as usize % 8;
        (self.get_mask_byte(byte_idx) & (1u8 << bit_idx)) != 0
    }

    /// Set a byte in the mask.
    #[inline]
    fn set_byte(&self, byte: u8) {
        let byte_idx = byte as usize / 8;
        let bit_idx = byte as usize % 8;
        let current = self.get_mask_byte(byte_idx);
        self.set_mask_byte(byte_idx, current | (1u8 << bit_idx));
    }

    /// Clear a byte in the mask.
    #[inline]
    fn clear_byte(&self, byte: u8) {
        let byte_idx = byte as usize / 8;
        let bit_idx = byte as usize % 8;
        let current = self.get_mask_byte(byte_idx);
        self.set_mask_byte(byte_idx, current & !(1u8 << bit_idx));
    }

    /// Get all bytes as a vector.
    pub fn get_bytes(&self, _arena: &mut ArenaAllocator) -> Vec<u8> {
        let count = self.get_count() as usize;
        let mut bytes = Vec::with_capacity(count);

        for i in 0u16..256 {
            if self.has_byte(i as u8) {
                bytes.push(i as u8);
            }
        }

        bytes
    }

    /// Get the first byte >= the given byte.
    pub fn get_next_byte(&self, byte: u8) -> Option<u8> {
        for i in byte as u16..256 {
            if self.has_byte(i as u8) {
                return Some(i as u8);
            }
        }
        None
    }

    /// Insert a byte.
    pub fn insert_byte(allocator: &mut FixedSizeAllocator, node: &mut Node, byte: u8) {
        let handle = Self::get_mut(allocator, *node);
        let count = handle.get_count();
        handle.set_count(count + 1);
        handle.set_byte(byte);
    }

    /// Delete a byte, shrinking to Node15Leaf if necessary.
    pub fn delete_byte(
        node15_allocator: &mut FixedSizeAllocator,
        node256_allocator: &mut FixedSizeAllocator,
        node: &mut Node,
        byte: u8,
    ) {
        let handle = Self::get_mut(node256_allocator, *node);
        let count = handle.get_count();
        handle.set_count(count - 1);
        handle.clear_byte(byte);

        if count - 1 > Self::SHRINK_THRESHOLD as u16 {
            return;
        }

        // Shrink to Node15Leaf
        let node256 = *node;
        Node15Leaf::shrink_from_node256(node15_allocator, node256_allocator, node, node256);
    }

    /// Grow from Node15Leaf.
    pub fn grow_from_node15(
        node15_allocator: &mut FixedSizeAllocator,
        node256_allocator: &mut FixedSizeAllocator,
        node256: &mut Node,
        node15: Node,
    ) {
        let n15 = Node15Leaf::get(node15_allocator, node15);

        Self::new(node256_allocator, node256);
        node256.set_gate_status(node15.get_gate_status());

        let n256 = Self::get_mut(node256_allocator, *node256);
        let count = n15.get_count();
        n256.set_count(count as u16);

        for i in 0..count {
            n256.set_byte(n15.get_byte(i));
        }

        node15_allocator.free(node15.into());
    }
}

// SAFETY: Node256Leaf is just a pointer to allocator-managed memory
unsafe impl Send for Node256Leaf {}
unsafe impl Sync for Node256Leaf {}

// Node4 is now in internal_node.rs

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::StandardBufferManager;
    use paro_common::allocator::{ArenaAllocator, DefaultAllocator};
    use std::sync::Arc;

    fn create_buffer_manager() -> Arc<StandardBufferManager> {
        Arc::new(StandardBufferManager::default_manager())
    }

    #[allow(dead_code)]
    fn create_arena() -> ArenaAllocator {
        let allocator = Arc::new(DefaultAllocator::new());
        ArenaAllocator::new(allocator)
    }

    fn create_node7_allocator() -> FixedSizeAllocator {
        let buffer_manager = create_buffer_manager();
        FixedSizeAllocator::with_buffer_manager(Node7Leaf::SIZE, 4096, buffer_manager)
    }

    fn create_node15_allocator() -> FixedSizeAllocator {
        let buffer_manager = create_buffer_manager();
        FixedSizeAllocator::with_buffer_manager(Node15Leaf::SIZE, 4096, buffer_manager)
    }

    fn create_node256_allocator() -> FixedSizeAllocator {
        let buffer_manager = create_buffer_manager();
        FixedSizeAllocator::with_buffer_manager(Node256Leaf::SIZE, 4096, buffer_manager)
    }

    // ========== Leaf Inlined Tests ==========

    #[test]
    fn test_leaf_new_inlined() {
        let mut node = Node::empty();
        Leaf::new(&mut node, 12345);

        assert_eq!(node.get_type(), NType::LeafInlined);
        assert_eq!(node.get_row_id(), 12345);
    }

    #[test]
    fn test_leaf_inlined_various_row_ids() {
        let row_ids = [0i64, 1, 100, 1000, 1_000_000, MAX_ROW_ID_LOCAL - 1];

        for &row_id in &row_ids {
            let mut node = Node::empty();
            Leaf::new(&mut node, row_id);
            assert_eq!(node.get_row_id(), row_id);
        }
    }

    // ========== Node7Leaf Tests ==========

    #[test]
    fn test_node7_leaf_new() {
        let mut allocator = create_node7_allocator();
        let mut node = Node::empty();

        Node7Leaf::new(&mut allocator, &mut node);

        assert_eq!(node.get_type(), NType::Node7Leaf);
        let handle = Node7Leaf::get(&allocator, node);
        assert_eq!(handle.get_count(), 0);
    }

    #[test]
    fn test_node7_leaf_insert_bytes() {
        let mut allocator = create_node7_allocator();
        let mut node = Node::empty();

        Node7Leaf::new(&mut allocator, &mut node);
        Node7Leaf::insert_byte_internal(&mut allocator, &mut node, 5);
        Node7Leaf::insert_byte_internal(&mut allocator, &mut node, 3);
        Node7Leaf::insert_byte_internal(&mut allocator, &mut node, 7);

        let handle = Node7Leaf::get(&allocator, node);
        assert_eq!(handle.get_count(), 3);
        // Should be sorted
        assert_eq!(handle.get_byte(0), 3);
        assert_eq!(handle.get_byte(1), 5);
        assert_eq!(handle.get_byte(2), 7);
    }

    #[test]
    fn test_node7_leaf_has_byte() {
        let mut allocator = create_node7_allocator();
        let mut node = Node::empty();

        Node7Leaf::new(&mut allocator, &mut node);
        Node7Leaf::insert_byte_internal(&mut allocator, &mut node, 10);
        Node7Leaf::insert_byte_internal(&mut allocator, &mut node, 20);

        let handle = Node7Leaf::get(&allocator, node);
        assert!(handle.has_byte(10));
        assert!(handle.has_byte(20));
        assert!(!handle.has_byte(15));
    }

    #[test]
    fn test_node7_leaf_get_next_byte() {
        let mut allocator = create_node7_allocator();
        let mut node = Node::empty();

        Node7Leaf::new(&mut allocator, &mut node);
        Node7Leaf::insert_byte_internal(&mut allocator, &mut node, 10);
        Node7Leaf::insert_byte_internal(&mut allocator, &mut node, 20);
        Node7Leaf::insert_byte_internal(&mut allocator, &mut node, 30);

        let handle = Node7Leaf::get(&allocator, node);
        assert_eq!(handle.get_next_byte(5), Some(10));
        assert_eq!(handle.get_next_byte(10), Some(10));
        assert_eq!(handle.get_next_byte(15), Some(20));
        assert_eq!(handle.get_next_byte(31), None);
    }

    #[test]
    fn test_node7_leaf_get_bytes() {
        let mut allocator = create_node7_allocator();
        let mut node = Node::empty();

        Node7Leaf::new(&mut allocator, &mut node);
        Node7Leaf::insert_byte_internal(&mut allocator, &mut node, 3);
        Node7Leaf::insert_byte_internal(&mut allocator, &mut node, 1);
        Node7Leaf::insert_byte_internal(&mut allocator, &mut node, 2);

        let handle = Node7Leaf::get(&allocator, node);
        let bytes = handle.get_bytes();
        assert_eq!(bytes, &[1, 2, 3]);
    }

    // ========== Node15Leaf Tests ==========

    #[test]
    fn test_node15_leaf_new() {
        let mut allocator = create_node15_allocator();
        let mut node = Node::empty();

        Node15Leaf::new(&mut allocator, &mut node);

        assert_eq!(node.get_type(), NType::Node15Leaf);
        let handle = Node15Leaf::get(&allocator, node);
        assert_eq!(handle.get_count(), 0);
    }

    #[test]
    fn test_node15_leaf_insert_bytes() {
        let mut allocator = create_node15_allocator();
        let mut node = Node::empty();

        Node15Leaf::new(&mut allocator, &mut node);
        for i in 0..10 {
            Node15Leaf::insert_byte_internal(&mut allocator, &mut node, (10 - i) as u8);
        }

        let handle = Node15Leaf::get(&allocator, node);
        assert_eq!(handle.get_count(), 10);
        // Should be sorted
        for i in 0..10 {
            assert_eq!(handle.get_byte(i), (i + 1) as u8);
        }
    }

    #[test]
    fn test_node15_leaf_grow_from_node7() {
        let mut node7_allocator = create_node7_allocator();
        let mut node15_allocator = create_node15_allocator();

        let mut node7 = Node::empty();
        Node7Leaf::new(&mut node7_allocator, &mut node7);
        for i in 0..7 {
            Node7Leaf::insert_byte_internal(&mut node7_allocator, &mut node7, i * 10);
        }
        node7.set_gate_status(GateStatus::Set);

        let mut node15 = Node::empty();
        Node15Leaf::grow_from_node7(
            &mut node7_allocator,
            &mut node15_allocator,
            &mut node15,
            node7,
        );

        assert_eq!(node15.get_type(), NType::Node15Leaf);
        assert_eq!(node15.get_gate_status(), GateStatus::Set);

        let handle = Node15Leaf::get(&node15_allocator, node15);
        assert_eq!(handle.get_count(), 7);
    }

    // ========== Node256Leaf Tests ==========

    #[test]
    fn test_node256_leaf_new() {
        let mut allocator = create_node256_allocator();
        let mut node = Node::empty();

        Node256Leaf::new(&mut allocator, &mut node);

        assert_eq!(node.get_type(), NType::Node256Leaf);
        let handle = Node256Leaf::get(&allocator, node);
        assert_eq!(handle.get_count(), 0);
    }

    #[test]
    fn test_node256_leaf_insert_bytes() {
        let mut allocator = create_node256_allocator();
        let mut node = Node::empty();

        Node256Leaf::new(&mut allocator, &mut node);
        Node256Leaf::insert_byte(&mut allocator, &mut node, 0);
        Node256Leaf::insert_byte(&mut allocator, &mut node, 128);
        Node256Leaf::insert_byte(&mut allocator, &mut node, 255);

        let handle = Node256Leaf::get(&allocator, node);
        assert_eq!(handle.get_count(), 3);
        assert!(handle.has_byte(0));
        assert!(handle.has_byte(128));
        assert!(handle.has_byte(255));
        assert!(!handle.has_byte(1));
    }

    #[test]
    fn test_node256_leaf_get_next_byte() {
        let mut allocator = create_node256_allocator();
        let mut node = Node::empty();

        Node256Leaf::new(&mut allocator, &mut node);
        Node256Leaf::insert_byte(&mut allocator, &mut node, 50);
        Node256Leaf::insert_byte(&mut allocator, &mut node, 100);
        Node256Leaf::insert_byte(&mut allocator, &mut node, 200);

        let handle = Node256Leaf::get(&allocator, node);
        assert_eq!(handle.get_next_byte(0), Some(50));
        assert_eq!(handle.get_next_byte(50), Some(50));
        assert_eq!(handle.get_next_byte(51), Some(100));
        assert_eq!(handle.get_next_byte(201), None);
    }

    #[test]
    fn test_node256_leaf_grow_from_node15() {
        let mut node15_allocator = create_node15_allocator();
        let mut node256_allocator = create_node256_allocator();

        let mut node15 = Node::empty();
        Node15Leaf::new(&mut node15_allocator, &mut node15);
        for i in 0..15 {
            Node15Leaf::insert_byte_internal(&mut node15_allocator, &mut node15, i * 10);
        }
        node15.set_gate_status(GateStatus::Set);

        let mut node256 = Node::empty();
        Node256Leaf::grow_from_node15(
            &mut node15_allocator,
            &mut node256_allocator,
            &mut node256,
            node15,
        );

        assert_eq!(node256.get_type(), NType::Node256Leaf);
        assert_eq!(node256.get_gate_status(), GateStatus::Set);

        let handle = Node256Leaf::get(&node256_allocator, node256);
        assert_eq!(handle.get_count(), 15);
        for i in 0..15 {
            assert!(handle.has_byte(i * 10));
        }
    }

    // ========== Shrink Tests ==========

    #[test]
    fn test_node7_shrink_from_node15() {
        let mut node7_allocator = create_node7_allocator();
        let mut node15_allocator = create_node15_allocator();

        let mut node15 = Node::empty();
        Node15Leaf::new(&mut node15_allocator, &mut node15);
        for i in 0..5 {
            Node15Leaf::insert_byte_internal(&mut node15_allocator, &mut node15, i * 10);
        }
        node15.set_gate_status(GateStatus::Set);

        let mut node7 = Node::empty();
        Node7Leaf::shrink_from_node15(
            &mut node7_allocator,
            &mut node15_allocator,
            &mut node7,
            node15,
        );

        assert_eq!(node7.get_type(), NType::Node7Leaf);
        assert_eq!(node7.get_gate_status(), GateStatus::Set);

        let handle = Node7Leaf::get(&node7_allocator, node7);
        assert_eq!(handle.get_count(), 5);
    }

    #[test]
    fn test_node15_shrink_from_node256() {
        let mut node15_allocator = create_node15_allocator();
        let mut node256_allocator = create_node256_allocator();

        let mut node256 = Node::empty();
        Node256Leaf::new(&mut node256_allocator, &mut node256);
        for i in 0..10 {
            Node256Leaf::insert_byte(&mut node256_allocator, &mut node256, i * 20);
        }
        node256.set_gate_status(GateStatus::Set);

        let mut node15 = Node::empty();
        Node15Leaf::shrink_from_node256(
            &mut node15_allocator,
            &mut node256_allocator,
            &mut node15,
            node256,
        );

        assert_eq!(node15.get_type(), NType::Node15Leaf);
        assert_eq!(node15.get_gate_status(), GateStatus::Set);

        let handle = Node15Leaf::get(&node15_allocator, node15);
        assert_eq!(handle.get_count(), 10);
    }

    // ========== Delete Tests ==========

    #[test]
    fn test_node15_delete_byte() {
        let mut node7_allocator = create_node7_allocator();
        let mut node15_allocator = create_node15_allocator();

        let mut node = Node::empty();
        Node15Leaf::new(&mut node15_allocator, &mut node);
        for i in 0..10 {
            Node15Leaf::insert_byte_internal(&mut node15_allocator, &mut node, i * 10);
        }

        // Delete middle byte
        Node15Leaf::delete_byte(&mut node7_allocator, &mut node15_allocator, &mut node, 50);

        let handle = Node15Leaf::get(&node15_allocator, node);
        assert_eq!(handle.get_count(), 9);
        assert!(!handle.has_byte(50));
    }

    #[test]
    fn test_node256_delete_byte() {
        let mut node15_allocator = create_node15_allocator();
        let mut node256_allocator = create_node256_allocator();

        let mut node = Node::empty();
        Node256Leaf::new(&mut node256_allocator, &mut node);
        for i in 0..20 {
            Node256Leaf::insert_byte(&mut node256_allocator, &mut node, i * 10);
        }

        // Delete a byte
        Node256Leaf::delete_byte(
            &mut node15_allocator,
            &mut node256_allocator,
            &mut node,
            100,
        );

        let handle = Node256Leaf::get(&node256_allocator, node);
        assert_eq!(handle.get_count(), 19);
        assert!(!handle.has_byte(100));
    }
}
