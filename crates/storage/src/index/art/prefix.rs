// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # ART Prefix Node - Prefix compression for common key prefixes
//!
//! ## Design
//! - Prefix nodes store common prefix bytes to compress the tree
//! - Each prefix node contains up to `prefix_count` bytes + 1 byte for count
//! - Prefix nodes form a linked list for longer prefixes
//! - The last byte of the prefix data stores the count of valid bytes
//! - A Node pointer follows the prefix data, pointing to the child node

use super::node::{GateStatus, NType, Node};
use super::ARTKey;
use crate::index::fixed_size_allocator::FixedSizeAllocator;

/// Size of row_id in bytes.
pub const ROW_ID_SIZE: usize = std::mem::size_of::<i64>();

/// Count value for row_id encoding (ROW_ID_SIZE - 1).
pub const ROW_ID_COUNT: u8 = (ROW_ID_SIZE - 1) as u8;

/// Deprecated prefix count (for backward compatibility).
pub const DEPRECATED_COUNT: u8 = 15;

/// Size of metadata (Node pointer + count byte).
pub const METADATA_SIZE: usize = std::mem::size_of::<Node>() + 1;

/// Prefix node for ART index.
///
/// A Prefix node stores common prefix bytes to compress the tree structure.
/// The layout in memory is:
/// ```text
/// +------------------+-------+------+
/// | prefix bytes     | count | Node |
/// | (prefix_count)   | (1B)  | (8B) |
/// +------------------+-------+------+
/// ```
///
/// The `count` byte stores the number of valid prefix bytes (0 to prefix_count).
/// The `Node` pointer points to the child node after the prefix.
pub struct Prefix {
    /// Pointer to the prefix data in the allocator.
    pub data: *mut u8,
    /// Pointer to the child Node (located after prefix bytes + count).
    pub ptr: *mut Node,
    /// Whether the prefix data is currently in memory.
    pub in_memory: bool,
}

impl Prefix {
    /// Create a new Prefix handle from an existing node pointer.
    ///
    /// # Arguments
    /// * `allocator` - The fixed-size allocator containing the prefix data
    /// * `ptr` - The node pointer to the prefix
    /// * `prefix_count` - The maximum number of prefix bytes (from ART configuration)
    /// * `is_mutable` - Whether the data should be marked as dirty
    ///
    /// # Safety
    /// The caller must ensure the node pointer is valid and points to a PREFIX node.
    pub fn new(
        allocator: &FixedSizeAllocator,
        ptr: Node,
        prefix_count: usize,
        is_mutable: bool,
    ) -> Self {
        debug_assert!(ptr.has_metadata());
        debug_assert_eq!(ptr.get_type(), NType::Prefix);

        let data = allocator.get(ptr.into(), is_mutable);
        // SAFETY: data layout is [prefix_bytes][count][Node]
        let node_ptr = unsafe { data.add(prefix_count + 1) as *mut Node };

        Self {
            data,
            ptr: node_ptr,
            in_memory: true,
        }
    }

    /// Create a Prefix handle with a specific count (for deprecated format).
    ///
    /// # Safety
    /// The caller must ensure the node pointer is valid.
    pub fn with_count(allocator: &FixedSizeAllocator, ptr: Node, count: usize) -> Self {
        let data = allocator.get(ptr.into(), true);
        // SAFETY: data layout is [prefix_bytes][count][Node]
        let node_ptr = unsafe { data.add(count + 1) as *mut Node };

        Self {
            data,
            ptr: node_ptr,
            in_memory: true,
        }
    }

    /// Try to create a Prefix handle, returning None if not in memory.
    ///
    /// This is used for checking if a prefix is loaded without forcing a load.
    pub fn try_new(allocator: &FixedSizeAllocator, ptr: Node, prefix_count: usize) -> Option<Self> {
        if !allocator.loaded_from_storage(ptr.into()) {
            return None;
        }

        Some(Self::new(allocator, ptr, prefix_count, false))
    }

    /// Get a byte at the given position.
    ///
    /// # Safety
    /// The position must be less than the prefix count.
    #[inline]
    pub fn get_byte(&self, pos: usize) -> u8 {
        // SAFETY: caller ensures pos is valid
        unsafe { *self.data.add(pos) }
    }

    /// Set a byte at the given position.
    ///
    /// # Safety
    /// The position must be less than the prefix count.
    #[inline]
    pub fn set_byte(&mut self, pos: usize, byte: u8) {
        // SAFETY: caller ensures pos is valid
        unsafe { *self.data.add(pos) = byte };
    }

    /// Get the count of valid prefix bytes.
    #[inline]
    pub fn get_count(&self, prefix_count: usize) -> u8 {
        // SAFETY: count is stored at offset prefix_count
        unsafe { *self.data.add(prefix_count) }
    }

    /// Set the count of valid prefix bytes.
    #[inline]
    pub fn set_count(&mut self, prefix_count: usize, count: u8) {
        // SAFETY: count is stored at offset prefix_count
        unsafe { *self.data.add(prefix_count) = count };
    }

    /// Get the child node pointer.
    #[inline]
    pub fn get_child(&self) -> Node {
        // SAFETY: ptr points to a valid Node
        unsafe { *self.ptr }
    }

    /// Set the child node pointer.
    #[inline]
    pub fn set_child(&mut self, child: Node) {
        // SAFETY: ptr points to a valid Node location
        unsafe { *self.ptr = child };
    }

    /// Clear the child node pointer.
    #[inline]
    pub fn clear_child(&mut self) {
        // SAFETY: ptr points to a valid Node location
        unsafe { (*self.ptr).clear() };
    }

    /// Get a byte from a prefix node without creating a full Prefix handle.
    ///
    /// This is a convenience method for quick byte access.
    pub fn get_byte_static(
        allocator: &FixedSizeAllocator,
        node: &Node,
        prefix_count: usize,
        pos: u8,
    ) -> u8 {
        debug_assert_eq!(node.get_type(), NType::Prefix);
        let prefix = Prefix::new(allocator, *node, prefix_count, false);
        prefix.get_byte(pos as usize)
    }

    // ========== Static Factory Methods ==========

    /// Create a new prefix node with the given data.
    ///
    /// # Arguments
    /// * `allocator` - The fixed-size allocator
    /// * `node` - Output: the new node pointer
    /// * `prefix_count` - Maximum prefix bytes
    /// * `data` - Source data to copy (can be null)
    /// * `count` - Number of bytes to copy
    /// * `offset` - Offset into source data
    ///
    /// # Returns
    /// A Prefix handle to the new node.
    fn new_internal(
        allocator: &mut FixedSizeAllocator,
        node: &mut Node,
        prefix_count: usize,
        data: *const u8,
        count: u8,
        offset: usize,
    ) -> Self {
        let ptr = allocator.new_segment();
        *node = Node::from_pointer(ptr);
        node.set_type(NType::Prefix);

        let mut prefix = Prefix::new(allocator, *node, prefix_count, true);
        prefix.set_count(prefix_count, count);

        if !data.is_null() && count > 0 {
            // SAFETY: data is valid and count bytes can be copied
            unsafe {
                std::ptr::copy_nonoverlapping(data.add(offset), prefix.data, count as usize);
            }
        }

        prefix.clear_child();
        prefix
    }

    /// Create a new chain of prefix nodes from a key.
    ///
    /// # Arguments
    /// * `allocator` - The fixed-size allocator
    /// * `node` - Output: will be set to the first node in the chain
    /// * `prefix_count` - Maximum prefix bytes per node
    /// * `key` - The key to create prefixes from
    /// * `depth` - Starting depth in the key
    /// * `count` - Total number of bytes to store
    ///
    /// # Returns
    /// A pointer to the child node at the end of the chain (where the leaf should be placed).
    pub fn create_chain(
        allocator: &mut FixedSizeAllocator,
        node: &mut Node,
        prefix_count: usize,
        key: &ARTKey,
        depth: usize,
        mut count: usize,
    ) -> *mut Node {
        if count == 0 {
            return node as *mut Node;
        }

        let mut offset = 0;
        let mut first_node: Option<Node> = None;
        let mut last_child_ptr: *mut Node = std::ptr::null_mut();

        // Create a temporary node to build the chain
        let mut temp_node = Node::empty();
        let mut current = &mut temp_node;

        while count > 0 {
            let this_count = count.min(prefix_count) as u8;
            let prefix = Self::new_internal(
                allocator,
                current,
                prefix_count,
                key.data,
                this_count,
                offset + depth,
            );

            if first_node.is_none() {
                first_node = Some(*current);
            }

            // Save the child pointer
            last_child_ptr = prefix.ptr;
            // Move to the child for the next iteration
            current = unsafe { &mut *prefix.ptr };
            offset += this_count as usize;
            count -= this_count as usize;
        }

        // Set node to the first prefix node
        if let Some(first) = first_node {
            *node = first;
        }

        last_child_ptr
    }

    /// Reduce the prefix by removing bytes from the beginning.
    ///
    /// This shifts all subsequent bytes and frees empty nodes.
    ///
    /// # Arguments
    /// * `allocator` - The fixed-size allocator
    /// * `node` - The prefix node to reduce (may be replaced)
    /// * `prefix_count` - Maximum prefix bytes
    /// * `pos` - Number of bytes to remove from the beginning
    pub fn reduce(
        allocator: &mut FixedSizeAllocator,
        node: &mut Node,
        prefix_count: usize,
        pos: usize,
    ) {
        debug_assert!(node.has_metadata());
        debug_assert!(pos < prefix_count);

        // Reducing always removes at least one byte, so gate status is cleared
        node.set_gate_status(GateStatus::NotSet);

        let mut prefix = Prefix::new(allocator, *node, prefix_count, true);
        let count = prefix.get_count(prefix_count) as usize;

        // If we're removing all but the last byte, just move to the child
        if pos == count - 1 {
            let next = prefix.get_child();
            Self::free_node(allocator, *node);
            *node = next;
            return;
        }

        // Shift remaining bytes to the beginning
        // FIXME: Could copy into new prefix chain instead of shifting
        for i in 0..(prefix_count - pos - 1) {
            let byte = prefix.get_byte(pos + i + 1);
            prefix.set_byte(i, byte);
        }

        prefix.set_count(prefix_count, (count - pos - 1) as u8);

        // Append remaining prefix nodes
        let child = prefix.get_child();
        prefix.append_chain(allocator, prefix_count, child);
    }

    /// Split the prefix at the given position.
    ///
    /// After splitting:
    /// - `node` references the node that replaces the split byte
    /// - `child` references the remaining node after the split
    ///
    /// # Returns
    /// `GateStatus::Set` if a gate node was freed, else `GateStatus::NotSet`.
    /// If it returns `GateStatus::Set`, the caller must set the gate for the
    /// node replacing the split byte after its creation.
    ///
    /// After split:
    /// - Case 1 (pos + 1 == prefix_count): `node` points to the prefix's child (where new Node4 goes)
    /// - Case 2/3 with pos == 0: `node` is cleared (the prefix was freed)
    /// - Case 2/3 with pos > 0: `node` still points to the original prefix (with reduced count)
    ///   The caller must update the prefix's child to point to the new internal node.
    pub fn split(
        allocator: &mut FixedSizeAllocator,
        node: &mut Node,
        child: &mut Node,
        prefix_count: usize,
        pos: u8,
    ) -> GateStatus {
        debug_assert!(node.has_metadata());

        let mut prefix = Prefix::new(allocator, *node, prefix_count, true);
        let count = prefix.get_count(prefix_count);
        let pos = pos as usize;

        // Case 1: Split at the last prefix byte, and prefix is full
        // The prefix count is reduced by 1, and node points to where the new Node4 should go
        if pos + 1 == prefix_count {
            prefix.set_count(prefix_count, count - 1);
            let next = prefix.get_child();
            *node = next;
            *child = next;
            return GateStatus::NotSet;
        }

        // Case 2: Split is not at the last prefix byte
        if pos + 1 < count as usize {
            // Create a new prefix with remaining bytes
            let mut new_prefix =
                Self::new_internal(allocator, child, prefix_count, std::ptr::null(), 0, 0);

            let remaining_count = count as usize - pos - 1;
            new_prefix.set_count(prefix_count, remaining_count as u8);

            // Copy remaining bytes
            for i in 0..remaining_count {
                let byte = prefix.get_byte(pos + 1 + i);
                new_prefix.set_byte(i, byte);
            }

            // Handle child node
            let prefix_child = prefix.get_child();
            if prefix_child.get_type() == NType::Prefix
                && prefix_child.get_gate_status() == GateStatus::NotSet
            {
                new_prefix.append_chain(allocator, prefix_count, prefix_child);
            } else {
                new_prefix.set_child(prefix_child);
            }
        } else {
            // Case 3: Split at the last prefix byte, but prefix is not full
            debug_assert_eq!(pos + 1, count as usize);
            *child = prefix.get_child();
        }

        // Set the new count of this node
        prefix.set_count(prefix_count, pos as u8);

        // No bytes left before the split, free this node
        if pos == 0 {
            let old_status = node.get_gate_status();
            Self::free_node(allocator, *node);
            node.clear();
            return old_status;
        }

        // There are bytes left before the split
        // node still points to the original prefix (with reduced count)
        // The caller must update the prefix's child to point to the new internal node
        GateStatus::NotSet
    }

    /// Append a single byte to this prefix.
    ///
    /// If the prefix is full, creates a new prefix node.
    ///
    /// # Returns
    /// A Prefix handle to the node where the byte was appended.
    fn append_byte(
        &mut self,
        allocator: &mut FixedSizeAllocator,
        prefix_count: usize,
        byte: u8,
    ) -> Prefix {
        let count = self.get_count(prefix_count) as usize;

        if count < prefix_count {
            // Room in current prefix
            self.set_byte(count, byte);
            self.set_count(prefix_count, (count + 1) as u8);
            return Prefix {
                data: self.data,
                ptr: self.ptr,
                in_memory: self.in_memory,
            };
        }

        // Prefix is full, create a new one
        let mut new_node = Node::empty();
        let mut new_prefix = Self::new_internal(
            allocator,
            &mut new_node,
            prefix_count,
            std::ptr::null(),
            0,
            0,
        );
        self.set_child(new_node);

        new_prefix.append_byte(allocator, prefix_count, byte)
    }

    /// Append another prefix chain to this prefix.
    ///
    /// Copies all bytes from the other prefix chain and frees the other nodes.
    fn append_chain(
        &mut self,
        allocator: &mut FixedSizeAllocator,
        prefix_count: usize,
        mut other: Node,
    ) {
        debug_assert!(other.has_metadata());

        let mut current = Prefix {
            data: self.data,
            ptr: self.ptr,
            in_memory: self.in_memory,
        };

        while other.get_type() == NType::Prefix {
            // Stop at gate boundaries
            if other.get_gate_status() == GateStatus::Set {
                current.set_child(other);
                return;
            }

            let other_prefix = Prefix::new(allocator, other, prefix_count, true);
            let other_count = other_prefix.get_count(prefix_count) as usize;

            // Copy all bytes from other prefix
            for i in 0..other_count {
                let byte = other_prefix.get_byte(i);
                current = current.append_byte(allocator, prefix_count, byte);
            }

            // Move to next and free current other
            let next = other_prefix.get_child();
            current.set_child(next);
            Self::free_node(allocator, other);
            other = next;
        }
    }

    /// Get the tail prefix in a chain.
    fn get_tail(allocator: &FixedSizeAllocator, node: Node, prefix_count: usize) -> Prefix {
        let mut prefix = Prefix::new(allocator, node, prefix_count, true);

        while prefix.get_child().get_type() == NType::Prefix {
            prefix = Prefix::new(allocator, prefix.get_child(), prefix_count, true);
        }

        prefix
    }

    /// Concatenate parent -> prev_node4 -> child.
    ///
    /// This is used when a Node4 is being removed and we need to merge
    /// the parent prefix with the child.
    ///
    /// # Arguments
    /// * `allocator` - The fixed-size allocator
    /// * `parent` - The parent node (may be a prefix)
    /// * `node4` - The Node4 being removed (will be replaced)
    /// * `child` - The child node to connect
    /// * `byte` - The byte from the Node4
    /// * `prefix_count` - Maximum prefix bytes
    /// * `node4_status` - Gate status of the Node4
    /// * `status` - Current gate status context
    pub fn concat(
        allocator: &mut FixedSizeAllocator,
        parent: &mut Node,
        node4: &mut Node,
        child: Node,
        byte: u8,
        prefix_count: usize,
        node4_status: GateStatus,
        status: GateStatus,
    ) {
        debug_assert!(!parent.is_any_leaf());
        debug_assert!(child.has_metadata());

        // Case 1: The Node4 was a gate
        if node4_status == GateStatus::Set {
            debug_assert_eq!(parent.get_gate_status(), GateStatus::NotSet);
            debug_assert_eq!(child.get_gate_status(), GateStatus::NotSet);
            Self::concat_node4_was_gate(allocator, node4, child, byte, prefix_count);
            return;
        }

        // Case 2: The child is a gate
        if child.get_gate_status() == GateStatus::Set {
            debug_assert_eq!(node4_status, GateStatus::NotSet);
            Self::concat_child_is_gate(allocator, parent, node4, child, byte, prefix_count);
            return;
        }

        // Case 3: Normal concatenation
        Self::concat_internal(allocator, parent, node4, child, byte, prefix_count, status);
    }

    /// Internal concatenation logic.
    fn concat_internal(
        allocator: &mut FixedSizeAllocator,
        parent: &mut Node,
        node4: &mut Node,
        child: Node,
        byte: u8,
        prefix_count: usize,
        status: GateStatus,
    ) {
        if child.get_type() == NType::LeafInlined {
            if status == GateStatus::Set {
                if parent.get_type() == NType::Prefix {
                    // Inline all the way up, gate is no longer nested
                    while parent.get_type() == NType::Prefix {
                        let prefix = Prefix::new(allocator, *parent, prefix_count, true);
                        let temp = prefix.get_child();
                        Self::free_node(allocator, *parent);
                        *parent = temp;
                    }
                    *parent = child;
                    return;
                }
                // Inside gate, inline directly into the previous Node4
                *node4 = child;
                return;
            }

            // Not inside a gate
            if parent.get_type() == NType::Prefix {
                // Append byte to prefix, then inline child
                let mut tail = Self::get_tail(allocator, *parent, prefix_count);
                tail = tail.append_byte(allocator, prefix_count, byte);
                tail.set_child(child);
                return;
            }

            // Create new prefix with the byte
            let mut new_prefix =
                Self::new_internal(allocator, node4, prefix_count, &byte as *const u8, 1, 0);
            new_prefix.set_child(child);
            return;
        }

        // Child is not inlined
        if parent.get_type() == NType::Prefix {
            // Append byte to prefix
            let mut tail = Self::get_tail(allocator, *parent, prefix_count);
            tail = tail.append_byte(allocator, prefix_count, byte);

            // Append child prefix chain if applicable
            if child.get_type() == NType::Prefix {
                tail.append_chain(allocator, prefix_count, child);
                return;
            }
            tail.set_child(child);
            return;
        }

        // Parent is not a prefix, create new prefix
        let mut new_prefix =
            Self::new_internal(allocator, node4, prefix_count, &byte as *const u8, 1, 0);
        if child.get_type() == NType::Prefix {
            new_prefix.append_chain(allocator, prefix_count, child);
            return;
        }
        new_prefix.set_child(child);
    }

    /// Handle concatenation when the Node4 was a gate.
    fn concat_node4_was_gate(
        allocator: &mut FixedSizeAllocator,
        node4: &mut Node,
        child: Node,
        byte: u8,
        prefix_count: usize,
    ) {
        if child.get_type() == NType::LeafInlined {
            // Inside gates, inlined row IDs are not prefixed
            *node4 = child;
            return;
        }

        if child.get_type() == NType::Prefix {
            // Create new prefix with the byte, then append child prefix
            let mut new_prefix =
                Self::new_internal(allocator, node4, prefix_count, &byte as *const u8, 1, 0);
            new_prefix.clear_child();
            new_prefix.append_chain(allocator, prefix_count, child);
            node4.set_gate_status(GateStatus::Set);
            return;
        }

        // Create new prefix with the byte, point to child
        let mut new_prefix =
            Self::new_internal(allocator, node4, prefix_count, &byte as *const u8, 1, 0);
        new_prefix.set_child(child);
        node4.set_gate_status(GateStatus::Set);
    }

    /// Handle concatenation when the child is a gate.
    fn concat_child_is_gate(
        allocator: &mut FixedSizeAllocator,
        parent: &mut Node,
        node4: &mut Node,
        child: Node,
        byte: u8,
        prefix_count: usize,
    ) {
        if parent.get_type() != NType::Prefix {
            // Create new prefix at the former position of Node4
            let mut new_prefix =
                Self::new_internal(allocator, node4, prefix_count, &byte as *const u8, 1, 0);
            new_prefix.set_child(child);
            return;
        }

        // Parent is a prefix chain, append byte to its tail
        let mut tail = Self::get_tail(allocator, *parent, prefix_count);
        tail = tail.append_byte(allocator, prefix_count, byte);
        tail.set_child(child);
    }

    /// Free a prefix node.
    pub fn free_node(allocator: &mut FixedSizeAllocator, node: Node) {
        debug_assert!(node.has_metadata());
        allocator.free(node.into());
    }

    /// Free an entire prefix chain and clear the node.
    ///
    /// This traverses the prefix chain and frees all prefix nodes,
    /// then clears the starting node.
    pub fn free_chain(allocator: &mut FixedSizeAllocator, node: &mut Node, prefix_count: usize) {
        let mut current = *node;
        while current.has_metadata() && current.get_type() == NType::Prefix {
            let prefix = Prefix::new(allocator, current, prefix_count, false);
            let next = prefix.get_child();
            Self::free_node(allocator, current);
            current = next;
        }
        node.clear();
    }

    /// Traverse a prefix chain, comparing with a key.
    ///
    /// # Arguments
    /// * `allocator` - The fixed-size allocator
    /// * `node` - The starting prefix node
    /// * `prefix_count` - Maximum prefix bytes
    /// * `key` - The key to compare against
    /// * `depth` - Current depth in the key
    ///
    /// # Returns
    /// - `Ok((new_depth, child))` if traversal succeeded
    /// - `Err(mismatch_pos)` if a mismatch was found
    pub fn traverse(
        allocator: &FixedSizeAllocator,
        mut node: Node,
        prefix_count: usize,
        key: &ARTKey,
        mut depth: usize,
    ) -> Result<(usize, Node), usize> {
        while node.get_type() == NType::Prefix {
            let prefix = Prefix::new(allocator, node, prefix_count, false);
            let count = prefix.get_count(prefix_count) as usize;

            // Compare prefix bytes with key
            for i in 0..count {
                if depth >= key.len {
                    return Err(depth);
                }
                if prefix.get_byte(i) != key.get(depth) {
                    return Err(depth);
                }
                depth += 1;
            }

            // Move to child
            node = prefix.get_child();

            // Stop at gate boundaries
            if node.get_gate_status() == GateStatus::Set {
                break;
            }
        }

        Ok((depth, node))
    }

    /// Iterate over all prefix nodes in a chain.
    ///
    /// # Arguments
    /// * `allocator` - The fixed-size allocator
    /// * `node` - The starting node
    /// * `prefix_count` - Maximum prefix bytes
    /// * `exit_gate` - Whether to stop at gate boundaries
    /// * `is_mutable` - Whether to mark data as dirty
    /// * `f` - Callback function for each prefix
    pub fn iterate<F>(
        allocator: &FixedSizeAllocator,
        mut node: Node,
        prefix_count: usize,
        exit_gate: bool,
        is_mutable: bool,
        mut f: F,
    ) where
        F: FnMut(&Prefix),
    {
        while node.has_metadata() && node.get_type() == NType::Prefix {
            let prefix = Prefix::new(allocator, node, prefix_count, is_mutable);
            f(&prefix);

            node = prefix.get_child();
            if exit_gate && node.get_gate_status() == GateStatus::Set {
                break;
            }
        }
    }

    /// Iterate mutably over all prefix nodes in a chain.
    pub fn iterate_mut<F>(
        allocator: &FixedSizeAllocator,
        mut node: Node,
        prefix_count: usize,
        exit_gate: bool,
        mut f: F,
    ) where
        F: FnMut(&mut Prefix),
    {
        while node.has_metadata() && node.get_type() == NType::Prefix {
            let mut prefix = Prefix::new(allocator, node, prefix_count, true);
            f(&mut prefix);

            node = prefix.get_child();
            if exit_gate && node.get_gate_status() == GateStatus::Set {
                break;
            }
        }
    }

    /// Get the total length of a prefix chain.
    pub fn get_chain_length(
        allocator: &FixedSizeAllocator,
        mut node: Node,
        prefix_count: usize,
    ) -> usize {
        let mut total = 0;

        while node.has_metadata() && node.get_type() == NType::Prefix {
            let prefix = Prefix::new(allocator, node, prefix_count, false);
            total += prefix.get_count(prefix_count) as usize;

            node = prefix.get_child();
            if node.get_gate_status() == GateStatus::Set {
                break;
            }
        }

        total
    }

    /// Copy prefix bytes to a buffer.
    ///
    /// # Returns
    /// The number of bytes copied.
    pub fn copy_to_buffer(
        allocator: &FixedSizeAllocator,
        mut node: Node,
        prefix_count: usize,
        buffer: &mut [u8],
    ) -> usize {
        let mut pos = 0;

        while node.has_metadata() && node.get_type() == NType::Prefix && pos < buffer.len() {
            let prefix = Prefix::new(allocator, node, prefix_count, false);
            let count = prefix.get_count(prefix_count) as usize;

            let copy_count = count.min(buffer.len() - pos);
            for i in 0..copy_count {
                buffer[pos + i] = prefix.get_byte(i);
            }
            pos += copy_count;

            node = prefix.get_child();
            if node.get_gate_status() == GateStatus::Set {
                break;
            }
        }

        pos
    }
}

// SAFETY: Prefix is just pointers to allocator-managed memory
unsafe impl Send for Prefix {}
unsafe impl Sync for Prefix {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::StandardBufferManager;
    use paro_common::allocator::{ArenaAllocator, DefaultAllocator};
    use std::sync::Arc;

    fn create_allocator() -> FixedSizeAllocator {
        let buffer_manager = Arc::new(StandardBufferManager::default_manager());
        // Prefix size: prefix_count bytes + 1 count byte + 8 bytes for Node
        // Using prefix_count = 15 (default)
        let segment_size = 15 + 1 + std::mem::size_of::<Node>();
        FixedSizeAllocator::with_buffer_manager(segment_size, 4096, buffer_manager)
    }

    fn create_arena() -> ArenaAllocator {
        let allocator = Arc::new(DefaultAllocator::new());
        ArenaAllocator::new(allocator)
    }

    const PREFIX_COUNT: usize = 15;

    #[test]
    fn test_prefix_new() {
        let mut allocator = create_allocator();
        let mut node = Node::empty();

        let prefix = Prefix::new_internal(
            &mut allocator,
            &mut node,
            PREFIX_COUNT,
            std::ptr::null(),
            0,
            0,
        );

        assert!(node.has_metadata());
        assert_eq!(node.get_type(), NType::Prefix);
        assert_eq!(prefix.get_count(PREFIX_COUNT), 0);
        assert!(prefix.in_memory);
    }

    #[test]
    fn test_prefix_set_get_byte() {
        let mut allocator = create_allocator();
        let mut node = Node::empty();

        let mut prefix = Prefix::new_internal(
            &mut allocator,
            &mut node,
            PREFIX_COUNT,
            std::ptr::null(),
            0,
            0,
        );

        prefix.set_byte(0, 0xAB);
        prefix.set_byte(1, 0xCD);
        prefix.set_count(PREFIX_COUNT, 2);

        assert_eq!(prefix.get_byte(0), 0xAB);
        assert_eq!(prefix.get_byte(1), 0xCD);
        assert_eq!(prefix.get_count(PREFIX_COUNT), 2);
    }

    #[test]
    fn test_prefix_child() {
        let mut allocator = create_allocator();
        let mut node = Node::empty();

        let mut prefix = Prefix::new_internal(
            &mut allocator,
            &mut node,
            PREFIX_COUNT,
            std::ptr::null(),
            0,
            0,
        );

        let child = Node::new(42, 100);
        prefix.set_child(child);

        let retrieved = prefix.get_child();
        assert_eq!(retrieved.get_buffer_id(), 42);
        assert_eq!(retrieved.get_offset(), 100);
    }

    #[test]
    fn test_prefix_create_from_data() {
        let mut allocator = create_allocator();
        let mut node = Node::empty();

        let data = [0x01, 0x02, 0x03, 0x04, 0x05];
        let prefix =
            Prefix::new_internal(&mut allocator, &mut node, PREFIX_COUNT, data.as_ptr(), 5, 0);

        assert_eq!(prefix.get_count(PREFIX_COUNT), 5);
        assert_eq!(prefix.get_byte(0), 0x01);
        assert_eq!(prefix.get_byte(1), 0x02);
        assert_eq!(prefix.get_byte(2), 0x03);
        assert_eq!(prefix.get_byte(3), 0x04);
        assert_eq!(prefix.get_byte(4), 0x05);
    }

    #[test]
    fn test_prefix_create_chain() {
        let mut allocator = create_allocator();
        let mut arena = create_arena();
        let mut node = Node::empty();

        // Create a key with 20 bytes (will need 2 prefix nodes with PREFIX_COUNT=15)
        let key = ARTKey::from_bytes(&mut arena, &[0u8; 20]).unwrap();

        let leaf_ptr = Prefix::create_chain(&mut allocator, &mut node, PREFIX_COUNT, &key, 0, 20);

        // After create_chain, node should point to the first prefix node
        assert!(node.has_metadata());
        assert_eq!(node.get_type(), NType::Prefix);

        // leaf_ptr should point to the child of the last prefix
        assert!(!leaf_ptr.is_null());

        // The chain should have been created
        assert!(allocator.total_segment_count() >= 2);
    }

    #[test]
    fn test_prefix_append_byte() {
        let mut allocator = create_allocator();
        let mut node = Node::empty();

        let mut prefix = Prefix::new_internal(
            &mut allocator,
            &mut node,
            PREFIX_COUNT,
            std::ptr::null(),
            0,
            0,
        );

        // Append bytes one by one
        for i in 0..5u8 {
            prefix = prefix.append_byte(&mut allocator, PREFIX_COUNT, i);
        }

        // Check the bytes were appended
        let check_prefix = Prefix::new(&allocator, node, PREFIX_COUNT, false);
        assert_eq!(check_prefix.get_count(PREFIX_COUNT), 5);
        for i in 0..5 {
            assert_eq!(check_prefix.get_byte(i), i as u8);
        }
    }

    #[test]
    fn test_prefix_append_byte_overflow() {
        let mut allocator = create_allocator();
        let mut node = Node::empty();

        let mut prefix = Prefix::new_internal(
            &mut allocator,
            &mut node,
            PREFIX_COUNT,
            std::ptr::null(),
            0,
            0,
        );

        // Fill the prefix completely
        for i in 0..PREFIX_COUNT as u8 {
            prefix = prefix.append_byte(&mut allocator, PREFIX_COUNT, i);
        }

        // Append one more byte - should create a new prefix node
        let initial_count = allocator.total_segment_count();
        let _ = prefix.append_byte(&mut allocator, PREFIX_COUNT, 0xFF);

        // Should have created a new segment
        assert!(allocator.total_segment_count() > initial_count);
    }

    #[test]
    fn test_prefix_get_byte_static() {
        let mut allocator = create_allocator();
        let mut node = Node::empty();

        let data = [0xAA, 0xBB, 0xCC];
        Prefix::new_internal(&mut allocator, &mut node, PREFIX_COUNT, data.as_ptr(), 3, 0);

        assert_eq!(
            Prefix::get_byte_static(&allocator, &node, PREFIX_COUNT, 0),
            0xAA
        );
        assert_eq!(
            Prefix::get_byte_static(&allocator, &node, PREFIX_COUNT, 1),
            0xBB
        );
        assert_eq!(
            Prefix::get_byte_static(&allocator, &node, PREFIX_COUNT, 2),
            0xCC
        );
    }

    #[test]
    fn test_prefix_traverse_success() {
        let mut allocator = create_allocator();
        let mut arena = create_arena();
        let mut node = Node::empty();

        // Create a prefix with bytes [1, 2, 3]
        let data = [1u8, 2, 3];
        let mut prefix =
            Prefix::new_internal(&mut allocator, &mut node, PREFIX_COUNT, data.as_ptr(), 3, 0);

        // Set a child node
        let mut child = Node::new(10, 20);
        child.set_type(NType::Node4);
        prefix.set_child(child);

        // Create a key directly with the same bytes (no escaping)
        // Use from_i64 which creates a fixed-size key
        let key = ARTKey::from_i64(&mut arena, 0x0102030405060708).unwrap();
        // The key bytes will be: [0x81, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
        // (sign bit flipped for i64)

        // Create a prefix that matches the key's first 3 bytes
        let key_slice = key.as_slice();
        let mut node2 = Node::empty();
        let mut prefix2 = Prefix::new_internal(
            &mut allocator,
            &mut node2,
            PREFIX_COUNT,
            key_slice.as_ptr(),
            3,
            0,
        );
        prefix2.set_child(child);

        let result = Prefix::traverse(&allocator, node2, PREFIX_COUNT, &key, 0);
        assert!(result.is_ok());

        let (depth, child_node) = result.unwrap();
        assert_eq!(depth, 3);
        assert_eq!(child_node.get_buffer_id(), 10);
    }

    #[test]
    fn test_prefix_traverse_mismatch() {
        let mut allocator = create_allocator();
        let mut arena = create_arena();
        let mut node = Node::empty();

        // Create a prefix with bytes [0x81, 0x02, 0x03] (matching i64 encoding)
        let data = [0x81u8, 0x02, 0x03];
        let mut prefix =
            Prefix::new_internal(&mut allocator, &mut node, PREFIX_COUNT, data.as_ptr(), 3, 0);

        // Set a child node
        let mut child = Node::new(10, 20);
        child.set_type(NType::Node4);
        prefix.set_child(child);

        // Create a key that doesn't match at position 1
        // 0x0109030405060708 -> [0x81, 0x09, 0x03, ...]
        let key = ARTKey::from_i64(&mut arena, 0x0109030405060708).unwrap();

        let result = Prefix::traverse(&allocator, node, PREFIX_COUNT, &key, 0);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), 1); // Mismatch at depth 1
    }

    #[test]
    fn test_prefix_get_chain_length() {
        let mut allocator = create_allocator();
        let mut node = Node::empty();

        // Create first prefix with 5 bytes
        let data1 = [1u8, 2, 3, 4, 5];
        let mut prefix1 = Prefix::new_internal(
            &mut allocator,
            &mut node,
            PREFIX_COUNT,
            data1.as_ptr(),
            5,
            0,
        );

        // Create second prefix with 3 bytes
        let mut node2 = Node::empty();
        let data2 = [6u8, 7, 8];
        let mut prefix2 = Prefix::new_internal(
            &mut allocator,
            &mut node2,
            PREFIX_COUNT,
            data2.as_ptr(),
            3,
            0,
        );

        // Link them
        prefix1.set_child(node2);

        // Set a non-prefix child for prefix2
        let mut child = Node::new(10, 20);
        child.set_type(NType::Node4);
        prefix2.set_child(child);

        let length = Prefix::get_chain_length(&allocator, node, PREFIX_COUNT);
        assert_eq!(length, 8); // 5 + 3
    }

    #[test]
    fn test_prefix_copy_to_buffer() {
        let mut allocator = create_allocator();
        let mut node = Node::empty();

        // Create prefix with bytes [0xAA, 0xBB, 0xCC, 0xDD]
        let data = [0xAAu8, 0xBB, 0xCC, 0xDD];
        let mut prefix =
            Prefix::new_internal(&mut allocator, &mut node, PREFIX_COUNT, data.as_ptr(), 4, 0);

        // Set a non-prefix child
        let mut child = Node::new(10, 20);
        child.set_type(NType::Node4);
        prefix.set_child(child);

        let mut buffer = [0u8; 10];
        let copied = Prefix::copy_to_buffer(&allocator, node, PREFIX_COUNT, &mut buffer);

        assert_eq!(copied, 4);
        assert_eq!(&buffer[..4], &[0xAA, 0xBB, 0xCC, 0xDD]);
    }

    #[test]
    fn test_prefix_iterate() {
        let mut allocator = create_allocator();
        let mut node = Node::empty();

        // Create first prefix
        let data1 = [1u8, 2, 3];
        let mut prefix1 = Prefix::new_internal(
            &mut allocator,
            &mut node,
            PREFIX_COUNT,
            data1.as_ptr(),
            3,
            0,
        );

        // Create second prefix
        let mut node2 = Node::empty();
        let data2 = [4u8, 5];
        let mut prefix2 = Prefix::new_internal(
            &mut allocator,
            &mut node2,
            PREFIX_COUNT,
            data2.as_ptr(),
            2,
            0,
        );

        // Link them
        prefix1.set_child(node2);

        // Set a non-prefix child for prefix2
        let mut child = Node::new(10, 20);
        child.set_type(NType::Node4);
        prefix2.set_child(child);

        // Count prefixes
        let mut count = 0;
        Prefix::iterate(&allocator, node, PREFIX_COUNT, false, false, |_| {
            count += 1;
        });

        assert_eq!(count, 2);
    }

    #[test]
    fn test_prefix_free_node() {
        let mut allocator = create_allocator();
        let mut node = Node::empty();

        Prefix::new_internal(
            &mut allocator,
            &mut node,
            PREFIX_COUNT,
            std::ptr::null(),
            0,
            0,
        );

        assert_eq!(allocator.total_segment_count(), 1);

        Prefix::free_node(&mut allocator, node);

        assert_eq!(allocator.total_segment_count(), 0);
    }

    #[test]
    fn test_prefix_split_at_last_byte_full() {
        let mut allocator = create_allocator();
        let mut node = Node::empty();

        // Create a full prefix (PREFIX_COUNT bytes)
        let data: Vec<u8> = (0..PREFIX_COUNT as u8).collect();
        let mut prefix = Prefix::new_internal(
            &mut allocator,
            &mut node,
            PREFIX_COUNT,
            data.as_ptr(),
            PREFIX_COUNT as u8,
            0,
        );

        // Set a child
        let mut child_node = Node::new(10, 20);
        child_node.set_type(NType::Node4);
        prefix.set_child(child_node);

        // Split at the last position
        let mut child = Node::empty();
        let status = Prefix::split(
            &mut allocator,
            &mut node,
            &mut child,
            PREFIX_COUNT,
            (PREFIX_COUNT - 1) as u8,
        );

        assert_eq!(status, GateStatus::NotSet);
        // node and child should both point to the original child
        assert_eq!(node.get_buffer_id(), 10);
        assert_eq!(child.get_buffer_id(), 10);
    }
}
