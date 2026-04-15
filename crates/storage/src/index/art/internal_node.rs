//! # ART Internal Nodes - Node4, Node16, Node48, Node256
//!
//! ## Design
//!
//! Internal nodes store children indexed by key bytes:
//! - Node4: 4 children, linear search, sorted keys
//! - Node16: 16 children, linear search, sorted keys
//! - Node48: 48 children, 256-byte index array for O(1) lookup
//! - Node256: 256 children, direct indexing by byte
//!
//! Growth: Node4 -> Node16 -> Node48 -> Node256
//! Shrink: Node256 -> Node48 -> Node16 -> Node4

use super::node::{GateStatus, NType, Node};
use super::prefix::Prefix;
use crate::index::fixed_size_allocator::FixedSizeAllocator;

// ============================================================================
// Node4 - Holds up to 4 children sorted by key byte
// ============================================================================

/// Node4 holds up to 4 children sorted by their key byte.
///
/// Memory layout:
/// ```text
/// +-------+--------+---------+----------------+
/// | count | key[4] | padding | children[4]    |
/// | (1B)  | (4B)   | (3B)    | (4 * 8B = 32B) |
/// +-------+--------+---------+----------------+
/// ```
pub struct Node4 {
    data: *mut u8,
}

impl Node4 {
    /// Capacity of Node4.
    pub const CAPACITY: usize = 4;

    /// Offset to children array (aligned to 8 bytes: 1 + 4 + 3 = 8).
    const CHILDREN_OFFSET: usize = 8;

    /// Size of Node4 in bytes.
    pub const SIZE: usize = Self::CHILDREN_OFFSET + Self::CAPACITY * std::mem::size_of::<Node>();

    /// Create a new Node4.
    pub fn new(allocator: &mut FixedSizeAllocator, node: &mut Node) {
        let ptr = allocator.new_segment();
        *node = Node::from_pointer(ptr);
        node.set_type(NType::Node4);

        let mut handle = Self::get_mut(allocator, *node);
        handle.set_count(0);
        // Zero-initialize keys and children
        for i in 0..Self::CAPACITY {
            handle.set_key(i as u8, 0);
            handle.set_child_internal(i as u8, Node::empty());
        }
    }

    /// Get a Node4 handle (immutable).
    pub fn get(allocator: &FixedSizeAllocator, node: Node) -> Self {
        debug_assert_eq!(node.get_type(), NType::Node4);
        let data = allocator.get(node.into(), false);
        Self { data }
    }

    /// Get a Node4 handle (mutable).
    pub fn get_mut(allocator: &FixedSizeAllocator, node: Node) -> Self {
        debug_assert_eq!(node.get_type(), NType::Node4);
        let data = allocator.get(node.into(), true);
        Self { data }
    }

    // ========== Field Accessors ==========

    /// Get the count of children.
    #[inline]
    pub fn get_count(&self) -> u8 {
        unsafe { *self.data }
    }

    /// Set the count of children.
    #[inline]
    fn set_count(&self, count: u8) {
        unsafe { *self.data = count };
    }

    /// Get a key at the given index.
    #[inline]
    pub fn get_key(&self, index: u8) -> u8 {
        debug_assert!((index as usize) < Self::CAPACITY);
        unsafe { *self.data.add(1 + index as usize) }
    }

    /// Set a key at the given index.
    #[inline]
    fn set_key(&self, index: u8, key: u8) {
        debug_assert!((index as usize) < Self::CAPACITY);
        unsafe { *self.data.add(1 + index as usize) = key };
    }

    /// Get a child at the given index.
    #[inline]
    pub fn get_child_at(&self, index: u8) -> Node {
        debug_assert!((index as usize) < Self::CAPACITY);
        unsafe {
            let ptr = self
                .data
                .add(Self::CHILDREN_OFFSET + index as usize * std::mem::size_of::<Node>())
                as *const Node;
            *ptr
        }
    }

    /// Set a child at the given index (internal).
    #[inline]
    fn set_child_internal(&mut self, index: u8, child: Node) {
        debug_assert!((index as usize) < Self::CAPACITY);
        unsafe {
            let ptr = self
                .data
                .add(Self::CHILDREN_OFFSET + index as usize * std::mem::size_of::<Node>())
                as *mut Node;
            *ptr = child;
        }
    }

    /// Get a mutable reference to a child at the given index.
    #[inline]
    pub fn get_child_mut(&mut self, index: u8) -> &mut Node {
        debug_assert!((index as usize) < Self::CAPACITY);
        unsafe {
            let ptr = self
                .data
                .add(Self::CHILDREN_OFFSET + index as usize * std::mem::size_of::<Node>())
                as *mut Node;
            &mut *ptr
        }
    }

    // ========== Child Operations ==========

    /// Get the child for a given byte.
    pub fn get_child(&self, byte: u8) -> Option<&Node> {
        let count = self.get_count();
        for i in 0..count {
            if self.get_key(i) == byte {
                let child = self.get_child_at(i);
                if !child.has_metadata() {
                    return None;
                }
                // Return reference to the child in the node
                return Some(unsafe {
                    let ptr = self
                        .data
                        .add(Self::CHILDREN_OFFSET + i as usize * std::mem::size_of::<Node>())
                        as *const Node;
                    &*ptr
                });
            }
        }
        None
    }

    /// Get a mutable child for a given byte.
    pub fn get_child_mutable(&mut self, byte: u8) -> Option<&mut Node> {
        let count = self.get_count();
        for i in 0..count {
            if self.get_key(i) == byte {
                return Some(self.get_child_mut(i));
            }
        }
        None
    }

    /// Get the first child >= the given byte.
    pub fn get_next_child(&self, byte: &mut u8) -> Option<&Node> {
        let count = self.get_count();
        for i in 0..count {
            let key = self.get_key(i);
            if key >= *byte {
                *byte = key;
                return Some(unsafe {
                    let ptr = self
                        .data
                        .add(Self::CHILDREN_OFFSET + i as usize * std::mem::size_of::<Node>())
                        as *const Node;
                    &*ptr
                });
            }
        }
        None
    }

    /// Replace the child at byte.
    pub fn replace_child(&mut self, byte: u8, child: Node) {
        let count = self.get_count();
        for i in 0..count {
            if self.get_key(i) == byte {
                let old_status = self.get_child_at(i).get_gate_status();
                self.set_child_internal(i, child);
                if old_status == GateStatus::Set && child.has_metadata() {
                    self.get_child_mut(i).set_gate_status(old_status);
                }
                return;
            }
        }
    }

    /// Insert a child at byte (internal, assumes space is available).
    pub(crate) fn insert_child_internal(&mut self, byte: u8, child: Node) {
        let count = self.get_count();

        // Find insertion position (maintain sorted order)
        let mut pos = 0u8;
        while pos < count && self.get_key(pos) < byte {
            pos += 1;
        }

        // Shift keys and children to make room
        for i in (pos..count).rev() {
            self.set_key(i + 1, self.get_key(i));
            self.set_child_internal(i + 1, self.get_child_at(i));
        }

        self.set_key(pos, byte);
        self.set_child_internal(pos, child);
        self.set_count(count + 1);
    }

    /// Insert a child, growing to Node16 if necessary.
    pub fn insert_child(
        node4_allocator: &mut FixedSizeAllocator,
        node16_allocator: &mut FixedSizeAllocator,
        node: &mut Node,
        byte: u8,
        child: Node,
    ) {
        let handle = Self::get(node4_allocator, *node);
        let count = handle.get_count() as usize;

        if count < Self::CAPACITY {
            let mut handle = Self::get_mut(node4_allocator, *node);
            handle.insert_child_internal(byte, child);
            return;
        }

        // Node is full, grow to Node16
        let node4 = *node;
        Node16::grow_from_node4(node4_allocator, node16_allocator, node, node4);
        Node16::insert_child(node16_allocator, node, byte, child);
    }

    /// Delete a child at byte.
    ///
    /// Returns the remaining child and its byte if only one child remains.
    fn delete_child_internal(&mut self, byte: u8) -> (u8, Option<(u8, Node)>) {
        let count = self.get_count();

        // Find the child position
        let mut pos = 0u8;
        while pos < count && self.get_key(pos) != byte {
            pos += 1;
        }

        // Shift remaining keys and children
        for i in pos..(count - 1) {
            self.set_key(i, self.get_key(i + 1));
            self.set_child_internal(i, self.get_child_at(i + 1));
        }
        self.set_count(count - 1);

        let new_count = count - 1;
        if new_count == 1 {
            // Return the remaining child for compression
            Some((self.get_key(0), self.get_child_at(0)))
        } else {
            None
        };

        (
            new_count,
            if new_count == 1 {
                Some((self.get_key(0), self.get_child_at(0)))
            } else {
                None
            },
        )
    }

    /// Delete a child, handling compression when only one child remains.
    pub fn delete_child(
        node4_allocator: &mut FixedSizeAllocator,
        prefix_allocator: &mut FixedSizeAllocator,
        node: &mut Node,
        prefix: &mut Node,
        byte: u8,
        status: GateStatus,
        prefix_count: usize,
    ) {
        let mut handle = Self::get_mut(node4_allocator, *node);
        let (new_count, remaining) = handle.delete_child_internal(byte);

        if new_count != 1 {
            return;
        }

        // Compress one-way nodes
        let (remaining_byte, child) = remaining.expect("Should have remaining child");
        let prev_node4_status = node.get_gate_status();

        node4_allocator.free((*node).into());

        // Concatenate prefix with the remaining child
        Prefix::concat(
            prefix_allocator,
            prefix,
            node,
            child,
            remaining_byte,
            prefix_count,
            prev_node4_status,
            status,
        );
    }

    /// Shrink from Node16.
    pub fn shrink_from_node16(
        node4_allocator: &mut FixedSizeAllocator,
        node16_allocator: &mut FixedSizeAllocator,
        node4: &mut Node,
        node16: Node,
    ) {
        Self::new(node4_allocator, node4);
        node4.set_gate_status(node16.get_gate_status());

        let mut n4 = Self::get_mut(node4_allocator, *node4);
        let n16 = Node16::get(node16_allocator, node16);

        let count = n16.get_count();
        n4.set_count(count);
        for i in 0..count {
            n4.set_key(i, n16.get_key(i));
            n4.set_child_internal(i, n16.get_child_at(i));
        }

        node16_allocator.free(node16.into());
    }

    /// Iterate over all children.
    pub fn iter_children<F>(&mut self, mut f: F)
    where
        F: FnMut(&mut Node),
    {
        let count = self.get_count();
        for i in 0..count {
            f(self.get_child_mut(i));
        }
    }
}

// SAFETY: Node4 is just a pointer to allocator-managed memory
unsafe impl Send for Node4 {}
unsafe impl Sync for Node4 {}

// ============================================================================
// Node16 - Holds up to 16 children sorted by key byte
// ============================================================================

/// Node16 holds up to 16 children sorted by their key byte.
///
/// Memory layout:
/// ```text
/// +-------+---------+---------+-----------------+
/// | count | key[16] | padding | children[16]    |
/// | (1B)  | (16B)   | (7B)    | (16 * 8B = 128B)|
/// +-------+---------+---------+-----------------+
/// ```
pub struct Node16 {
    data: *mut u8,
}

impl Node16 {
    /// Capacity of Node16.
    pub const CAPACITY: usize = 16;

    /// Offset to children array (aligned to 8 bytes: 1 + 16 + 7 = 24).
    const CHILDREN_OFFSET: usize = 24;

    /// Size of Node16 in bytes.
    pub const SIZE: usize = Self::CHILDREN_OFFSET + Self::CAPACITY * std::mem::size_of::<Node>();

    /// Create a new Node16.
    pub fn new(allocator: &mut FixedSizeAllocator, node: &mut Node) {
        let ptr = allocator.new_segment();
        *node = Node::from_pointer(ptr);
        node.set_type(NType::Node16);

        let mut handle = Self::get_mut(allocator, *node);
        handle.set_count(0);
        // Zero-initialize keys and children
        for i in 0..Self::CAPACITY {
            handle.set_key(i as u8, 0);
            handle.set_child_internal(i as u8, Node::empty());
        }
    }

    /// Get a Node16 handle (immutable).
    pub fn get(allocator: &FixedSizeAllocator, node: Node) -> Self {
        debug_assert_eq!(node.get_type(), NType::Node16);
        let data = allocator.get(node.into(), false);
        Self { data }
    }

    /// Get a Node16 handle (mutable).
    pub fn get_mut(allocator: &FixedSizeAllocator, node: Node) -> Self {
        debug_assert_eq!(node.get_type(), NType::Node16);
        let data = allocator.get(node.into(), true);
        Self { data }
    }

    // ========== Field Accessors ==========

    /// Get the count of children.
    #[inline]
    pub fn get_count(&self) -> u8 {
        unsafe { *self.data }
    }

    /// Set the count of children.
    #[inline]
    fn set_count(&self, count: u8) {
        unsafe { *self.data = count };
    }

    /// Get a key at the given index.
    #[inline]
    pub fn get_key(&self, index: u8) -> u8 {
        debug_assert!((index as usize) < Self::CAPACITY);
        unsafe { *self.data.add(1 + index as usize) }
    }

    /// Set a key at the given index.
    #[inline]
    fn set_key(&self, index: u8, key: u8) {
        debug_assert!((index as usize) < Self::CAPACITY);
        unsafe { *self.data.add(1 + index as usize) = key };
    }

    /// Get a child at the given index.
    #[inline]
    pub fn get_child_at(&self, index: u8) -> Node {
        debug_assert!((index as usize) < Self::CAPACITY);
        unsafe {
            let ptr = self
                .data
                .add(Self::CHILDREN_OFFSET + index as usize * std::mem::size_of::<Node>())
                as *const Node;
            *ptr
        }
    }

    /// Set a child at the given index (internal).
    #[inline]
    fn set_child_internal(&mut self, index: u8, child: Node) {
        debug_assert!((index as usize) < Self::CAPACITY);
        unsafe {
            let ptr = self
                .data
                .add(Self::CHILDREN_OFFSET + index as usize * std::mem::size_of::<Node>())
                as *mut Node;
            *ptr = child;
        }
    }

    /// Get a mutable reference to a child at the given index.
    #[inline]
    pub fn get_child_mut(&mut self, index: u8) -> &mut Node {
        debug_assert!((index as usize) < Self::CAPACITY);
        unsafe {
            let ptr = self
                .data
                .add(Self::CHILDREN_OFFSET + index as usize * std::mem::size_of::<Node>())
                as *mut Node;
            &mut *ptr
        }
    }

    // ========== Child Operations ==========

    /// Get the child for a given byte.
    pub fn get_child(&self, byte: u8) -> Option<&Node> {
        let count = self.get_count();
        for i in 0..count {
            if self.get_key(i) == byte {
                let child = self.get_child_at(i);
                if !child.has_metadata() {
                    return None;
                }
                return Some(unsafe {
                    let ptr = self
                        .data
                        .add(Self::CHILDREN_OFFSET + i as usize * std::mem::size_of::<Node>())
                        as *const Node;
                    &*ptr
                });
            }
        }
        None
    }

    /// Get a mutable child for a given byte.
    pub fn get_child_mutable(&mut self, byte: u8) -> Option<&mut Node> {
        let count = self.get_count();
        for i in 0..count {
            if self.get_key(i) == byte {
                return Some(self.get_child_mut(i));
            }
        }
        None
    }

    /// Get the first child >= the given byte.
    pub fn get_next_child(&self, byte: &mut u8) -> Option<&Node> {
        let count = self.get_count();
        for i in 0..count {
            let key = self.get_key(i);
            if key >= *byte {
                *byte = key;
                return Some(unsafe {
                    let ptr = self
                        .data
                        .add(Self::CHILDREN_OFFSET + i as usize * std::mem::size_of::<Node>())
                        as *const Node;
                    &*ptr
                });
            }
        }
        None
    }

    /// Replace the child at byte.
    pub fn replace_child(&mut self, byte: u8, child: Node) {
        let count = self.get_count();
        for i in 0..count {
            if self.get_key(i) == byte {
                let old_status = self.get_child_at(i).get_gate_status();
                self.set_child_internal(i, child);
                if old_status == GateStatus::Set && child.has_metadata() {
                    self.get_child_mut(i).set_gate_status(old_status);
                }
                return;
            }
        }
    }

    /// Insert a child at byte (internal, assumes space is available).
    pub fn insert_child_internal(&mut self, byte: u8, child: Node) {
        let count = self.get_count();

        // Find insertion position (maintain sorted order)
        let mut pos = 0u8;
        while pos < count && self.get_key(pos) < byte {
            pos += 1;
        }

        // Shift keys and children to make room
        for i in (pos..count).rev() {
            self.set_key(i + 1, self.get_key(i));
            self.set_child_internal(i + 1, self.get_child_at(i));
        }

        self.set_key(pos, byte);
        self.set_child_internal(pos, child);
        self.set_count(count + 1);
    }

    /// Insert a child (single allocator version, assumes space is available).
    pub fn insert_child(
        allocator: &mut FixedSizeAllocator,
        node: &mut Node,
        byte: u8,
        child: Node,
    ) {
        let mut handle = Self::get_mut(allocator, *node);
        let count = handle.get_count() as usize;

        if count < Self::CAPACITY {
            handle.insert_child_internal(byte, child);
            return;
        }

        panic!("Node16 is full, should grow to Node48");
    }

    /// Insert a child, growing to Node48 if necessary.
    pub fn insert_child_with_growth(
        node16_allocator: &mut FixedSizeAllocator,
        node48_allocator: &mut FixedSizeAllocator,
        node: &mut Node,
        byte: u8,
        child: Node,
    ) {
        let handle = Self::get_mut(node16_allocator, *node);
        let count = handle.get_count() as usize;

        if count < Self::CAPACITY {
            let mut handle = Self::get_mut(node16_allocator, *node);
            handle.insert_child_internal(byte, child);
            return;
        }

        // Node is full, grow to Node48
        let node16 = *node;
        Node48::grow_from_node16(node16_allocator, node48_allocator, node, node16);
        Node48::insert_child(node48_allocator, node, byte, child);
    }

    /// Delete a child at byte (internal).
    fn delete_child_internal(&mut self, byte: u8) -> u8 {
        let count = self.get_count();

        // Find the child position
        let mut pos = 0u8;
        while pos < count && self.get_key(pos) != byte {
            pos += 1;
        }

        // Shift remaining keys and children
        for i in pos..(count - 1) {
            self.set_key(i, self.get_key(i + 1));
            self.set_child_internal(i, self.get_child_at(i + 1));
        }
        self.set_count(count - 1);

        count - 1
    }

    /// Delete a child, shrinking to Node4 if necessary.
    pub fn delete_child(
        node4_allocator: &mut FixedSizeAllocator,
        node16_allocator: &mut FixedSizeAllocator,
        node: &mut Node,
        byte: u8,
    ) {
        let mut handle = Self::get_mut(node16_allocator, *node);
        let new_count = handle.delete_child_internal(byte);

        if new_count >= Node4::CAPACITY as u8 {
            return;
        }

        // Shrink to Node4
        let node16 = *node;
        Node4::shrink_from_node16(node4_allocator, node16_allocator, node, node16);
    }

    /// Grow from Node4.
    pub fn grow_from_node4(
        node4_allocator: &mut FixedSizeAllocator,
        node16_allocator: &mut FixedSizeAllocator,
        node16: &mut Node,
        node4: Node,
    ) {
        let n4 = Node4::get(node4_allocator, node4);

        Self::new(node16_allocator, node16);
        node16.set_gate_status(node4.get_gate_status());

        let mut n16 = Self::get_mut(node16_allocator, *node16);
        let count = n4.get_count();
        n16.set_count(count);
        for i in 0..count {
            n16.set_key(i, n4.get_key(i));
            n16.set_child_internal(i, n4.get_child_at(i));
        }

        node4_allocator.free(node4.into());
    }

    /// Shrink from Node48.
    pub fn shrink_from_node48(
        node16_allocator: &mut FixedSizeAllocator,
        node48_allocator: &mut FixedSizeAllocator,
        node16: &mut Node,
        node48: Node,
    ) {
        Self::new(node16_allocator, node16);
        node16.set_gate_status(node48.get_gate_status());

        let mut n16 = Self::get_mut(node16_allocator, *node16);
        let n48 = Node48::get(node48_allocator, node48);

        let mut count = 0u8;
        for i in 0u16..256 {
            let idx = n48.get_child_index(i as u8);
            if idx != Node48::EMPTY_MARKER {
                n16.set_key(count, i as u8);
                n16.set_child_internal(count, n48.get_child_at(idx));
                count += 1;
            }
        }
        n16.set_count(count);

        node48_allocator.free(node48.into());
    }

    /// Iterate over all children.
    pub fn iter_children<F>(&mut self, mut f: F)
    where
        F: FnMut(&mut Node),
    {
        let count = self.get_count();
        for i in 0..count {
            f(self.get_child_mut(i));
        }
    }
}

// SAFETY: Node16 is just a pointer to allocator-managed memory
unsafe impl Send for Node16 {}
unsafe impl Sync for Node16 {}

// ============================================================================
// Node48 - Holds up to 48 children with 256-byte index array
// ============================================================================

/// Node48 holds up to 48 children. The child_index array is indexed by the key byte.
/// It contains the position of the child node in the children array.
///
/// Memory layout:
/// ```text
/// +-------+------------------+---------+----------------+
/// | count | child_index[256] | padding | children[48]   |
/// | (1B)  | (256B)           | (7B)    | (48 * 8B = 384B)|
/// +-------+------------------+---------+----------------+
/// ```
pub struct Node48 {
    data: *mut u8,
}

impl Node48 {
    /// Capacity of Node48.
    pub const CAPACITY: usize = 48;

    /// Empty marker in child_index array.
    pub const EMPTY_MARKER: u8 = 48;

    /// Shrink threshold (shrink to Node16 when count drops below this).
    pub const SHRINK_THRESHOLD: usize = 12;

    /// Offset to children array (aligned to 8 bytes: 1 + 256 + 7 = 264).
    const CHILDREN_OFFSET: usize = 264;

    /// Size of Node48 in bytes.
    pub const SIZE: usize = Self::CHILDREN_OFFSET + Self::CAPACITY * std::mem::size_of::<Node>();

    /// Create a new Node48.
    pub fn new(allocator: &mut FixedSizeAllocator, node: &mut Node) {
        let ptr = allocator.new_segment();
        *node = Node::from_pointer(ptr);
        node.set_type(NType::Node48);

        let mut handle = Self::get_mut(allocator, *node);
        handle.set_count(0);
        // Initialize child_index to EMPTY_MARKER
        for i in 0..256 {
            handle.set_child_index(i as u8, Self::EMPTY_MARKER);
        }
        // Zero-initialize children
        for i in 0..Self::CAPACITY {
            handle.set_child_internal(i as u8, Node::empty());
        }
    }

    /// Get a Node48 handle (immutable).
    pub fn get(allocator: &FixedSizeAllocator, node: Node) -> Self {
        debug_assert_eq!(node.get_type(), NType::Node48);
        let data = allocator.get(node.into(), false);
        Self { data }
    }

    /// Get a Node48 handle (mutable).
    pub fn get_mut(allocator: &FixedSizeAllocator, node: Node) -> Self {
        debug_assert_eq!(node.get_type(), NType::Node48);
        let data = allocator.get(node.into(), true);
        Self { data }
    }

    // ========== Field Accessors ==========

    /// Get the count of children.
    #[inline]
    pub fn get_count(&self) -> u8 {
        unsafe { *self.data }
    }

    /// Set the count of children.
    #[inline]
    fn set_count(&self, count: u8) {
        unsafe { *self.data = count };
    }

    /// Get the child index for a byte.
    #[inline]
    pub fn get_child_index(&self, byte: u8) -> u8 {
        unsafe { *self.data.add(1 + byte as usize) }
    }

    /// Set the child index for a byte.
    #[inline]
    fn set_child_index(&self, byte: u8, index: u8) {
        unsafe { *self.data.add(1 + byte as usize) = index };
    }

    /// Get a child at the given index.
    #[inline]
    pub fn get_child_at(&self, index: u8) -> Node {
        debug_assert!((index as usize) < Self::CAPACITY);
        unsafe {
            let ptr = self
                .data
                .add(Self::CHILDREN_OFFSET + index as usize * std::mem::size_of::<Node>())
                as *const Node;
            *ptr
        }
    }

    /// Set a child at the given index (internal).
    #[inline]
    fn set_child_internal(&mut self, index: u8, child: Node) {
        debug_assert!((index as usize) < Self::CAPACITY);
        unsafe {
            let ptr = self
                .data
                .add(Self::CHILDREN_OFFSET + index as usize * std::mem::size_of::<Node>())
                as *mut Node;
            *ptr = child;
        }
    }

    /// Get a mutable reference to a child at the given index.
    #[inline]
    pub fn get_child_mut(&mut self, index: u8) -> &mut Node {
        debug_assert!((index as usize) < Self::CAPACITY);
        unsafe {
            let ptr = self
                .data
                .add(Self::CHILDREN_OFFSET + index as usize * std::mem::size_of::<Node>())
                as *mut Node;
            &mut *ptr
        }
    }

    // ========== Child Operations ==========

    /// Get the child for a given byte.
    pub fn get_child(&self, byte: u8) -> Option<&Node> {
        let idx = self.get_child_index(byte);
        if idx != Self::EMPTY_MARKER {
            let child = self.get_child_at(idx);
            if !child.has_metadata() {
                return None;
            }
            return Some(unsafe {
                let ptr = self
                    .data
                    .add(Self::CHILDREN_OFFSET + idx as usize * std::mem::size_of::<Node>())
                    as *const Node;
                &*ptr
            });
        }
        None
    }

    /// Get a mutable child for a given byte.
    pub fn get_child_mutable(&mut self, byte: u8) -> Option<&mut Node> {
        let idx = self.get_child_index(byte);
        if idx != Self::EMPTY_MARKER {
            return Some(self.get_child_mut(idx));
        }
        None
    }

    /// Get the first child >= the given byte.
    pub fn get_next_child(&self, byte: &mut u8) -> Option<&Node> {
        for i in (*byte as usize)..256 {
            let idx = self.get_child_index(i as u8);
            if idx != Self::EMPTY_MARKER {
                *byte = i as u8;
                return Some(unsafe {
                    let ptr = self
                        .data
                        .add(Self::CHILDREN_OFFSET + idx as usize * std::mem::size_of::<Node>())
                        as *const Node;
                    &*ptr
                });
            }
        }
        None
    }

    /// Replace the child at byte.
    pub fn replace_child(&mut self, byte: u8, child: Node) {
        let idx = self.get_child_index(byte);
        debug_assert!(idx != Self::EMPTY_MARKER);
        let old_status = self.get_child_at(idx).get_gate_status();
        self.set_child_internal(idx, child);
        if old_status == GateStatus::Set && child.has_metadata() {
            self.get_child_mut(idx).set_gate_status(old_status);
        }
    }

    /// Insert a child (single allocator version).
    pub fn insert_child(
        allocator: &mut FixedSizeAllocator,
        node: &mut Node,
        byte: u8,
        child: Node,
    ) {
        let mut handle = Self::get_mut(allocator, *node);
        let count = handle.get_count();

        if (count as usize) < Self::CAPACITY {
            // Find an empty position in the children array
            let mut child_pos = count;
            if handle.get_child_at(child_pos).has_metadata() {
                child_pos = 0;
                while handle.get_child_at(child_pos).has_metadata() {
                    child_pos += 1;
                }
            }

            handle.set_child_internal(child_pos, child);
            handle.set_child_index(byte, child_pos);
            handle.set_count(count + 1);
            return;
        }

        panic!("Node48 is full, should grow to Node256");
    }

    /// Insert a child, growing to Node256 if necessary.
    pub fn insert_child_with_growth(
        node48_allocator: &mut FixedSizeAllocator,
        node256_allocator: &mut FixedSizeAllocator,
        node: &mut Node,
        byte: u8,
        child: Node,
    ) {
        let handle = Self::get(node48_allocator, *node);
        let count = handle.get_count() as usize;

        if count < Self::CAPACITY {
            Self::insert_child(node48_allocator, node, byte, child);
            return;
        }

        // Node is full, grow to Node256
        let node48 = *node;
        Node256::grow_from_node48(node48_allocator, node256_allocator, node, node48);
        Node256::insert_child(node256_allocator, node, byte, child);
    }

    /// Delete a child, shrinking to Node16 if necessary.
    pub fn delete_child(
        node16_allocator: &mut FixedSizeAllocator,
        node48_allocator: &mut FixedSizeAllocator,
        node: &mut Node,
        byte: u8,
    ) {
        let mut handle = Self::get_mut(node48_allocator, *node);
        let idx = handle.get_child_index(byte);

        // Clear the child
        handle.set_child_internal(idx, Node::empty());
        handle.set_child_index(byte, Self::EMPTY_MARKER);
        let count = handle.get_count();
        handle.set_count(count - 1);

        if (count - 1) as usize >= Self::SHRINK_THRESHOLD {
            return;
        }

        // Shrink to Node16
        let node48 = *node;
        Node16::shrink_from_node48(node16_allocator, node48_allocator, node, node48);
    }

    /// Grow from Node16.
    pub fn grow_from_node16(
        node16_allocator: &mut FixedSizeAllocator,
        node48_allocator: &mut FixedSizeAllocator,
        node48: &mut Node,
        node16: Node,
    ) {
        let n16 = Node16::get(node16_allocator, node16);

        Self::new(node48_allocator, node48);
        node48.set_gate_status(node16.get_gate_status());

        let mut n48 = Self::get_mut(node48_allocator, *node48);
        let count = n16.get_count();
        n48.set_count(count);

        for i in 0..count {
            let key = n16.get_key(i);
            n48.set_child_index(key, i);
            n48.set_child_internal(i, n16.get_child_at(i));
        }

        node16_allocator.free(node16.into());
    }

    /// Shrink from Node256.
    pub fn shrink_from_node256(
        node48_allocator: &mut FixedSizeAllocator,
        node256_allocator: &mut FixedSizeAllocator,
        node48: &mut Node,
        node256: Node,
    ) {
        Self::new(node48_allocator, node48);
        node48.set_gate_status(node256.get_gate_status());

        let mut n48 = Self::get_mut(node48_allocator, *node48);
        let n256 = Node256::get(node256_allocator, node256);

        let mut count = 0u8;
        for i in 0u16..256 {
            let child = n256.get_child_at(i as u8);
            if child.has_metadata() {
                n48.set_child_index(i as u8, count);
                n48.set_child_internal(count, child);
                count += 1;
            }
        }
        n48.set_count(count);

        node256_allocator.free(node256.into());
    }

    /// Iterate over all children.
    pub fn iter_children<F>(&mut self, mut f: F)
    where
        F: FnMut(&mut Node),
    {
        for i in 0u16..256 {
            let idx = self.get_child_index(i as u8);
            if idx != Self::EMPTY_MARKER {
                f(self.get_child_mut(idx));
            }
        }
    }
}

// SAFETY: Node48 is just a pointer to allocator-managed memory
unsafe impl Send for Node48 {}
unsafe impl Sync for Node48 {}

// ============================================================================
// Node256 - Holds up to 256 children indexed directly by byte
// ============================================================================

/// Node256 holds up to 256 children. They are indexed directly by their key byte.
///
/// Memory layout:
/// ```text
/// +-------+---------+-------------------+
/// | count | padding | children[256]     |
/// | (2B)  | (6B)    | (256 * 8B = 2048B)|
/// +-------+---------+-------------------+
/// ```
pub struct Node256 {
    data: *mut u8,
}

impl Node256 {
    /// Capacity of Node256.
    pub const CAPACITY: usize = 256;

    /// Shrink threshold (shrink to Node48 when count drops to this).
    pub const SHRINK_THRESHOLD: usize = 36;

    /// Offset to children array (aligned to 8 bytes).
    const CHILDREN_OFFSET: usize = 8;

    /// Size of Node256 in bytes.
    pub const SIZE: usize = Self::CHILDREN_OFFSET + Self::CAPACITY * std::mem::size_of::<Node>();

    /// Create a new Node256.
    pub fn new(allocator: &mut FixedSizeAllocator, node: &mut Node) {
        let ptr = allocator.new_segment();
        *node = Node::from_pointer(ptr);
        node.set_type(NType::Node256);

        let mut handle = Self::get_mut(allocator, *node);
        handle.set_count(0);
        // Zero-initialize children
        for i in 0..Self::CAPACITY {
            handle.set_child_internal(i as u8, Node::empty());
        }
    }

    /// Get a Node256 handle (immutable).
    pub fn get(allocator: &FixedSizeAllocator, node: Node) -> Self {
        debug_assert_eq!(node.get_type(), NType::Node256);
        let data = allocator.get(node.into(), false);
        Self { data }
    }

    /// Get a Node256 handle (mutable).
    pub fn get_mut(allocator: &FixedSizeAllocator, node: Node) -> Self {
        debug_assert_eq!(node.get_type(), NType::Node256);
        let data = allocator.get(node.into(), true);
        Self { data }
    }

    // ========== Field Accessors ==========

    /// Get the count of children.
    #[inline]
    pub fn get_count(&self) -> u16 {
        // Read as two bytes to avoid alignment issues
        unsafe {
            let lo = *self.data as u16;
            let hi = (*self.data.add(1) as u16) << 8;
            lo | hi
        }
    }

    /// Set the count of children.
    #[inline]
    fn set_count(&self, count: u16) {
        // Write as two bytes to avoid alignment issues
        unsafe {
            *self.data = count as u8;
            *self.data.add(1) = (count >> 8) as u8;
        }
    }

    /// Get a child at the given byte index.
    #[inline]
    pub fn get_child_at(&self, byte: u8) -> Node {
        unsafe {
            let ptr = self
                .data
                .add(Self::CHILDREN_OFFSET + byte as usize * std::mem::size_of::<Node>())
                as *const Node;
            *ptr
        }
    }

    /// Set a child at the given byte index (internal).
    #[inline]
    fn set_child_internal(&mut self, byte: u8, child: Node) {
        unsafe {
            let ptr = self
                .data
                .add(Self::CHILDREN_OFFSET + byte as usize * std::mem::size_of::<Node>())
                as *mut Node;
            *ptr = child;
        }
    }

    /// Get a mutable reference to a child at the given byte index.
    #[inline]
    pub fn get_child_mut(&mut self, byte: u8) -> &mut Node {
        unsafe {
            let ptr = self
                .data
                .add(Self::CHILDREN_OFFSET + byte as usize * std::mem::size_of::<Node>())
                as *mut Node;
            &mut *ptr
        }
    }

    // ========== Child Operations ==========

    /// Get the child for a given byte.
    pub fn get_child(&self, byte: u8) -> Option<&Node> {
        let child = self.get_child_at(byte);
        if child.has_metadata() {
            return Some(unsafe {
                let ptr = self
                    .data
                    .add(Self::CHILDREN_OFFSET + byte as usize * std::mem::size_of::<Node>())
                    as *const Node;
                &*ptr
            });
        }
        None
    }

    /// Get a mutable child for a given byte.
    pub fn get_child_mutable(&mut self, byte: u8) -> Option<&mut Node> {
        let child = self.get_child_at(byte);
        if child.has_metadata() {
            return Some(self.get_child_mut(byte));
        }
        None
    }

    /// Get the first child >= the given byte.
    pub fn get_next_child(&self, byte: &mut u8) -> Option<&Node> {
        for i in (*byte as usize)..Self::CAPACITY {
            let child = self.get_child_at(i as u8);
            if child.has_metadata() {
                *byte = i as u8;
                return Some(unsafe {
                    let ptr = self
                        .data
                        .add(Self::CHILDREN_OFFSET + i * std::mem::size_of::<Node>())
                        as *const Node;
                    &*ptr
                });
            }
        }
        None
    }

    /// Replace the child at byte.
    pub fn replace_child(&mut self, byte: u8, child: Node) {
        let old_status = self.get_child_at(byte).get_gate_status();
        self.set_child_internal(byte, child);
        if old_status == GateStatus::Set && child.has_metadata() {
            self.get_child_mut(byte).set_gate_status(old_status);
        }
    }

    /// Insert a child.
    pub fn insert_child(
        allocator: &mut FixedSizeAllocator,
        node: &mut Node,
        byte: u8,
        child: Node,
    ) {
        let mut handle = Self::get_mut(allocator, *node);
        let count = handle.get_count();
        handle.set_count(count + 1);
        handle.set_child_internal(byte, child);
    }

    /// Delete a child, shrinking to Node48 if necessary.
    pub fn delete_child(
        node48_allocator: &mut FixedSizeAllocator,
        node256_allocator: &mut FixedSizeAllocator,
        node: &mut Node,
        byte: u8,
    ) {
        let mut handle = Self::get_mut(node256_allocator, *node);

        // Clear the child
        handle.set_child_internal(byte, Node::empty());
        let count = handle.get_count();
        handle.set_count(count - 1);

        if (count - 1) as usize > Self::SHRINK_THRESHOLD {
            return;
        }

        // Shrink to Node48
        let node256 = *node;
        Node48::shrink_from_node256(node48_allocator, node256_allocator, node, node256);
    }

    /// Grow from Node48.
    pub fn grow_from_node48(
        node48_allocator: &mut FixedSizeAllocator,
        node256_allocator: &mut FixedSizeAllocator,
        node256: &mut Node,
        node48: Node,
    ) {
        let n48 = Node48::get(node48_allocator, node48);

        Self::new(node256_allocator, node256);
        node256.set_gate_status(node48.get_gate_status());

        let mut n256 = Self::get_mut(node256_allocator, *node256);
        n256.set_count(n48.get_count() as u16);

        for i in 0u16..256 {
            let idx = n48.get_child_index(i as u8);
            if idx != Node48::EMPTY_MARKER {
                n256.set_child_internal(i as u8, n48.get_child_at(idx));
            }
        }

        node48_allocator.free(node48.into());
    }

    /// Iterate over all children.
    pub fn iter_children<F>(&mut self, mut f: F)
    where
        F: FnMut(&mut Node),
    {
        for i in 0u16..Self::CAPACITY as u16 {
            let child = self.get_child_at(i as u8);
            if child.has_metadata() {
                f(self.get_child_mut(i as u8));
            }
        }
    }
}

// SAFETY: Node256 is just a pointer to allocator-managed memory
unsafe impl Send for Node256 {}
unsafe impl Sync for Node256 {}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::StandardBufferManager;
    use std::sync::Arc;

    fn create_test_allocator(segment_size: usize) -> FixedSizeAllocator {
        let buffer_manager = Arc::new(StandardBufferManager::default_manager());
        FixedSizeAllocator::with_buffer_manager(segment_size, 4096, buffer_manager)
    }

    // ========== Node4 Tests ==========

    #[test]
    fn test_node4_new() {
        let mut allocator = create_test_allocator(Node4::SIZE);
        let mut node = Node::empty();
        Node4::new(&mut allocator, &mut node);

        assert_eq!(node.get_type(), NType::Node4);
        let handle = Node4::get(&allocator, node);
        assert_eq!(handle.get_count(), 0);
    }

    fn create_child_node(buffer_id: u32, offset: u32) -> Node {
        let mut node = Node::new(buffer_id, offset);
        node.set_type(NType::LeafInlined); // Set a type so has_metadata() returns true
        node
    }

    #[test]
    fn test_node4_insert_and_get() {
        let mut allocator = create_test_allocator(Node4::SIZE);
        let mut node = Node::empty();
        Node4::new(&mut allocator, &mut node);

        // Insert children
        let mut handle = Node4::get_mut(&allocator, node);
        handle.insert_child_internal(10, create_child_node(1, 100));
        handle.insert_child_internal(5, create_child_node(2, 200));
        handle.insert_child_internal(20, create_child_node(3, 300));

        assert_eq!(handle.get_count(), 3);

        // Verify sorted order
        assert_eq!(handle.get_key(0), 5);
        assert_eq!(handle.get_key(1), 10);
        assert_eq!(handle.get_key(2), 20);

        // Get children
        let child = handle.get_child(10);
        assert!(child.is_some());
        assert_eq!(child.unwrap().get_buffer_id(), 1);

        let child = handle.get_child(5);
        assert!(child.is_some());
        assert_eq!(child.unwrap().get_buffer_id(), 2);

        let child = handle.get_child(100);
        assert!(child.is_none());
    }

    #[test]
    fn test_node4_get_next_child() {
        let mut allocator = create_test_allocator(Node4::SIZE);
        let mut node = Node::empty();
        Node4::new(&mut allocator, &mut node);

        let mut handle = Node4::get_mut(&allocator, node);
        handle.insert_child_internal(10, create_child_node(1, 100));
        handle.insert_child_internal(20, create_child_node(2, 200));
        handle.insert_child_internal(30, create_child_node(3, 300));

        let mut byte = 0u8;
        let child = handle.get_next_child(&mut byte);
        assert!(child.is_some());
        assert_eq!(byte, 10);

        byte = 15;
        let child = handle.get_next_child(&mut byte);
        assert!(child.is_some());
        assert_eq!(byte, 20);

        byte = 31;
        let child = handle.get_next_child(&mut byte);
        assert!(child.is_none());
    }

    #[test]
    fn test_node4_replace_child() {
        let mut allocator = create_test_allocator(Node4::SIZE);
        let mut node = Node::empty();
        Node4::new(&mut allocator, &mut node);

        let mut handle = Node4::get_mut(&allocator, node);
        handle.insert_child_internal(10, create_child_node(1, 100));

        // Replace child
        handle.replace_child(10, create_child_node(99, 999));

        let child = handle.get_child(10);
        assert!(child.is_some());
        assert_eq!(child.unwrap().get_buffer_id(), 99);
    }

    // ========== Node16 Tests ==========

    #[test]
    fn test_node16_new() {
        let mut allocator = create_test_allocator(Node16::SIZE);
        let mut node = Node::empty();
        Node16::new(&mut allocator, &mut node);

        assert_eq!(node.get_type(), NType::Node16);
        let handle = Node16::get(&allocator, node);
        assert_eq!(handle.get_count(), 0);
    }

    #[test]
    fn test_node16_insert_and_get() {
        let mut allocator = create_test_allocator(Node16::SIZE);
        let mut node = Node::empty();
        Node16::new(&mut allocator, &mut node);

        let mut handle = Node16::get_mut(&allocator, node);
        for i in 0..10 {
            handle.insert_child_internal(i * 10, create_child_node(i as u32, i as u32 * 100));
        }

        assert_eq!(handle.get_count(), 10);

        // Verify sorted order
        for i in 0..10 {
            assert_eq!(handle.get_key(i), i * 10);
        }

        // Get children
        let child = handle.get_child(50);
        assert!(child.is_some());
        assert_eq!(child.unwrap().get_buffer_id(), 5);
    }

    #[test]
    fn test_node4_to_node16_growth() {
        let mut node4_allocator = create_test_allocator(Node4::SIZE);
        let mut node16_allocator = create_test_allocator(Node16::SIZE);

        let mut node = Node::empty();
        Node4::new(&mut node4_allocator, &mut node);

        // Fill Node4
        for i in 0..4 {
            let mut handle = Node4::get_mut(&node4_allocator, node);
            handle.insert_child_internal(i * 10, create_child_node(i as u32, i as u32 * 100));
        }

        // Insert one more to trigger growth
        Node4::insert_child(
            &mut node4_allocator,
            &mut node16_allocator,
            &mut node,
            50,
            create_child_node(5, 500),
        );

        assert_eq!(node.get_type(), NType::Node16);
        let handle = Node16::get(&node16_allocator, node);
        assert_eq!(handle.get_count(), 5);
    }

    // ========== Node48 Tests ==========

    #[test]
    fn test_node48_new() {
        let mut allocator = create_test_allocator(Node48::SIZE);
        let mut node = Node::empty();
        Node48::new(&mut allocator, &mut node);

        assert_eq!(node.get_type(), NType::Node48);
        let handle = Node48::get(&allocator, node);
        assert_eq!(handle.get_count(), 0);
    }

    #[test]
    fn test_node48_insert_and_get() {
        let mut allocator = create_test_allocator(Node48::SIZE);
        let mut node = Node::empty();
        Node48::new(&mut allocator, &mut node);

        // Insert children
        for i in 0..20 {
            Node48::insert_child(
                &mut allocator,
                &mut node,
                i * 10,
                create_child_node(i as u32, i as u32 * 100),
            );
        }

        let handle = Node48::get(&allocator, node);
        assert_eq!(handle.get_count(), 20);

        // Get children
        let child = handle.get_child(50);
        assert!(child.is_some());
        assert_eq!(child.unwrap().get_buffer_id(), 5);

        let child = handle.get_child(55);
        assert!(child.is_none());
    }

    #[test]
    fn test_node48_get_next_child() {
        let mut allocator = create_test_allocator(Node48::SIZE);
        let mut node = Node::empty();
        Node48::new(&mut allocator, &mut node);

        Node48::insert_child(&mut allocator, &mut node, 10, create_child_node(1, 100));
        Node48::insert_child(&mut allocator, &mut node, 50, create_child_node(2, 200));
        Node48::insert_child(&mut allocator, &mut node, 200, create_child_node(3, 300));

        let handle = Node48::get(&allocator, node);

        let mut byte = 0u8;
        let child = handle.get_next_child(&mut byte);
        assert!(child.is_some());
        assert_eq!(byte, 10);

        byte = 11;
        let child = handle.get_next_child(&mut byte);
        assert!(child.is_some());
        assert_eq!(byte, 50);

        byte = 201;
        let child = handle.get_next_child(&mut byte);
        assert!(child.is_none());
    }

    #[test]
    fn test_node16_to_node48_growth() {
        let mut node16_allocator = create_test_allocator(Node16::SIZE);
        let mut node48_allocator = create_test_allocator(Node48::SIZE);

        let mut node = Node::empty();
        Node16::new(&mut node16_allocator, &mut node);

        // Fill Node16
        for i in 0..16 {
            Node16::insert_child(
                &mut node16_allocator,
                &mut node,
                i * 10,
                create_child_node(i as u32, i as u32 * 100),
            );
        }

        // Insert one more to trigger growth
        Node16::insert_child_with_growth(
            &mut node16_allocator,
            &mut node48_allocator,
            &mut node,
            200,
            create_child_node(17, 1700),
        );

        assert_eq!(node.get_type(), NType::Node48);
        let handle = Node48::get(&node48_allocator, node);
        assert_eq!(handle.get_count(), 17);
    }

    // ========== Node256 Tests ==========

    #[test]
    fn test_node256_new() {
        let mut allocator = create_test_allocator(Node256::SIZE);
        let mut node = Node::empty();
        Node256::new(&mut allocator, &mut node);

        assert_eq!(node.get_type(), NType::Node256);
        let handle = Node256::get(&allocator, node);
        assert_eq!(handle.get_count(), 0);
    }

    #[test]
    fn test_node256_insert_and_get() {
        let mut allocator = create_test_allocator(Node256::SIZE);
        let mut node = Node::empty();
        Node256::new(&mut allocator, &mut node);

        // Insert children
        for i in 0..100 {
            Node256::insert_child(
                &mut allocator,
                &mut node,
                i,
                create_child_node(i as u32, i as u32 * 10),
            );
        }

        let handle = Node256::get(&allocator, node);
        assert_eq!(handle.get_count(), 100);

        // Get children
        let child = handle.get_child(50);
        assert!(child.is_some());
        assert_eq!(child.unwrap().get_buffer_id(), 50);

        let child = handle.get_child(150);
        assert!(child.is_none());
    }

    #[test]
    fn test_node256_get_next_child() {
        let mut allocator = create_test_allocator(Node256::SIZE);
        let mut node = Node::empty();
        Node256::new(&mut allocator, &mut node);

        Node256::insert_child(&mut allocator, &mut node, 10, create_child_node(1, 100));
        Node256::insert_child(&mut allocator, &mut node, 100, create_child_node(2, 200));
        Node256::insert_child(&mut allocator, &mut node, 200, create_child_node(3, 300));

        let handle = Node256::get(&allocator, node);

        let mut byte = 0u8;
        let child = handle.get_next_child(&mut byte);
        assert!(child.is_some());
        assert_eq!(byte, 10);

        byte = 50;
        let child = handle.get_next_child(&mut byte);
        assert!(child.is_some());
        assert_eq!(byte, 100);

        byte = 201;
        let child = handle.get_next_child(&mut byte);
        assert!(child.is_none());
    }

    #[test]
    fn test_node48_to_node256_growth() {
        let mut node48_allocator = create_test_allocator(Node48::SIZE);
        let mut node256_allocator = create_test_allocator(Node256::SIZE);

        let mut node = Node::empty();
        Node48::new(&mut node48_allocator, &mut node);

        // Fill Node48
        for i in 0..48 {
            Node48::insert_child(
                &mut node48_allocator,
                &mut node,
                i * 5,
                create_child_node(i as u32, i as u32 * 100),
            );
        }

        // Insert one more to trigger growth
        Node48::insert_child_with_growth(
            &mut node48_allocator,
            &mut node256_allocator,
            &mut node,
            250,
            create_child_node(49, 4900),
        );

        assert_eq!(node.get_type(), NType::Node256);
        let handle = Node256::get(&node256_allocator, node);
        assert_eq!(handle.get_count(), 49);
    }

    // ========== Shrink Tests ==========

    #[test]
    fn test_node16_to_node4_shrink() {
        let mut node4_allocator = create_test_allocator(Node4::SIZE);
        let mut node16_allocator = create_test_allocator(Node16::SIZE);

        let mut node = Node::empty();
        Node16::new(&mut node16_allocator, &mut node);

        // Insert 5 children
        for i in 0..5 {
            Node16::insert_child(
                &mut node16_allocator,
                &mut node,
                i * 10,
                create_child_node(i as u32, i as u32 * 100),
            );
        }

        // Delete until we trigger shrink (below Node4::CAPACITY)
        Node16::delete_child(&mut node4_allocator, &mut node16_allocator, &mut node, 40);
        assert_eq!(node.get_type(), NType::Node16);

        Node16::delete_child(&mut node4_allocator, &mut node16_allocator, &mut node, 30);
        assert_eq!(node.get_type(), NType::Node4);

        let handle = Node4::get(&node4_allocator, node);
        assert_eq!(handle.get_count(), 3);
    }

    #[test]
    fn test_node48_to_node16_shrink() {
        let mut node16_allocator = create_test_allocator(Node16::SIZE);
        let mut node48_allocator = create_test_allocator(Node48::SIZE);

        let mut node = Node::empty();
        Node48::new(&mut node48_allocator, &mut node);

        // Insert 12 children (at shrink threshold)
        for i in 0..12 {
            Node48::insert_child(
                &mut node48_allocator,
                &mut node,
                i * 10,
                create_child_node(i as u32, i as u32 * 100),
            );
        }

        // Delete one to trigger shrink (count becomes 11, below threshold of 12)
        Node48::delete_child(&mut node16_allocator, &mut node48_allocator, &mut node, 110);

        assert_eq!(node.get_type(), NType::Node16);
        let handle = Node16::get(&node16_allocator, node);
        assert_eq!(handle.get_count(), 11);
    }

    #[test]
    fn test_node256_to_node48_shrink() {
        let mut node48_allocator = create_test_allocator(Node48::SIZE);
        let mut node256_allocator = create_test_allocator(Node256::SIZE);

        let mut node = Node::empty();
        Node256::new(&mut node256_allocator, &mut node);

        // Insert 37 children (just above shrink threshold)
        for i in 0..37 {
            Node256::insert_child(
                &mut node256_allocator,
                &mut node,
                i,
                create_child_node(i as u32, i as u32 * 100),
            );
        }

        // Delete one to trigger shrink
        Node256::delete_child(&mut node48_allocator, &mut node256_allocator, &mut node, 36);

        assert_eq!(node.get_type(), NType::Node48);
        let handle = Node48::get(&node48_allocator, node);
        assert_eq!(handle.get_count(), 36);
    }

    // ========== Gate Status Tests ==========

    #[test]
    fn test_gate_status_preserved_on_growth() {
        let mut node4_allocator = create_test_allocator(Node4::SIZE);
        let mut node16_allocator = create_test_allocator(Node16::SIZE);

        let mut node = Node::empty();
        Node4::new(&mut node4_allocator, &mut node);
        node.set_gate_status(GateStatus::Set);

        // Fill Node4
        for i in 0..4 {
            let mut handle = Node4::get_mut(&node4_allocator, node);
            handle.insert_child_internal(i * 10, create_child_node(i as u32, i as u32 * 100));
        }

        // Grow to Node16
        Node4::insert_child(
            &mut node4_allocator,
            &mut node16_allocator,
            &mut node,
            50,
            create_child_node(5, 500),
        );

        assert_eq!(node.get_type(), NType::Node16);
        assert_eq!(node.get_gate_status(), GateStatus::Set);
    }

    #[test]
    fn test_gate_status_preserved_on_replace() {
        let mut allocator = create_test_allocator(Node4::SIZE);
        let mut node = Node::empty();
        Node4::new(&mut allocator, &mut node);

        let mut handle = Node4::get_mut(&allocator, node);
        let mut child = Node::new(1, 100);
        child.set_type(NType::Node4);
        child.set_gate_status(GateStatus::Set);
        handle.insert_child_internal(10, child);

        // Replace with a new child (gate status should be preserved)
        let mut new_child = Node::new(2, 200);
        new_child.set_type(NType::Node4);
        handle.replace_child(10, new_child);

        let replaced = handle.get_child(10).unwrap();
        assert_eq!(replaced.get_gate_status(), GateStatus::Set);
    }

    // ========== Iterator Tests ==========

    #[test]
    fn test_node4_iter_children() {
        let mut allocator = create_test_allocator(Node4::SIZE);
        let mut node = Node::empty();
        Node4::new(&mut allocator, &mut node);

        let mut handle = Node4::get_mut(&allocator, node);
        handle.insert_child_internal(10, create_child_node(1, 100));
        handle.insert_child_internal(20, create_child_node(2, 200));
        handle.insert_child_internal(30, create_child_node(3, 300));

        let mut count = 0;
        handle.iter_children(|_child| {
            count += 1;
        });
        assert_eq!(count, 3);
    }

    #[test]
    fn test_node48_iter_children() {
        let mut allocator = create_test_allocator(Node48::SIZE);
        let mut node = Node::empty();
        Node48::new(&mut allocator, &mut node);

        for i in 0..20 {
            Node48::insert_child(
                &mut allocator,
                &mut node,
                i * 10,
                create_child_node(i as u32, i as u32 * 100),
            );
        }

        let mut handle = Node48::get_mut(&allocator, node);
        let mut count = 0;
        handle.iter_children(|_child| {
            count += 1;
        });
        assert_eq!(count, 20);
    }

    #[test]
    fn test_node256_iter_children() {
        let mut allocator = create_test_allocator(Node256::SIZE);
        let mut node = Node::empty();
        Node256::new(&mut allocator, &mut node);

        for i in 0..50 {
            Node256::insert_child(
                &mut allocator,
                &mut node,
                i * 5,
                create_child_node(i as u32, i as u32 * 100),
            );
        }

        let mut handle = Node256::get_mut(&allocator, node);
        let mut count = 0;
        handle.iter_children(|_child| {
            count += 1;
        });
        assert_eq!(count, 50);
    }
}
