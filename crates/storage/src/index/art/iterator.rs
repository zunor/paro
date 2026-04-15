//! # ART Iterator - Tree traversal for range scans
//!
//! ## Design
//!
//! The Iterator provides tree traversal for range scans:
//! - Maintains a stack of nodes from root to current position
//! - Tracks the current key bytes leading to the current node
//! - Supports finding minimum, lower bound, and next operations
//! - Handles nested leaves (gate nodes) for duplicate keys

use std::collections::BTreeSet;

use super::internal_node::{Node16, Node256, Node4, Node48};
use super::leaf::{Node15Leaf, Node256Leaf, Node7Leaf};
use super::node::{GateStatus, NType, Node, ALLOCATOR_COUNT};
use super::prefix::{Prefix, ROW_ID_SIZE};
use super::ARTKey;
use crate::index::fixed_size_allocator::FixedSizeAllocator;

/// Entry in the iterator stack.
#[derive(Debug, Clone, Copy)]
pub struct IteratorEntry {
    /// The node at this level.
    pub node: Node,
    /// The byte leading to the currently active child.
    pub byte: u8,
}

impl IteratorEntry {
    /// Create a new iterator entry.
    pub fn new(node: Node, byte: u8) -> Self {
        Self { node, byte }
    }
}

/// Tracks the current key in the iterator.
pub struct IteratorKey {
    /// Key bytes from root to current position.
    key_bytes: Vec<u8>,
}

impl IteratorKey {
    /// Create a new empty iterator key.
    pub fn new() -> Self {
        Self {
            key_bytes: Vec::new(),
        }
    }

    /// Push a byte into the current key.
    #[inline]
    pub fn push(&mut self, byte: u8) {
        self.key_bytes.push(byte);
    }

    /// Pop n bytes from the current key.
    #[inline]
    pub fn pop(&mut self, n: usize) {
        let new_len = self.key_bytes.len().saturating_sub(n);
        self.key_bytes.truncate(new_len);
    }

    /// Get the byte at index.
    #[inline]
    pub fn get(&self, idx: usize) -> u8 {
        self.key_bytes[idx]
    }

    /// Get the number of key bytes.
    #[inline]
    pub fn len(&self) -> usize {
        self.key_bytes.len()
    }

    /// Check if the key is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.key_bytes.is_empty()
    }

    /// Check if key_bytes contains all bytes of key.
    pub fn contains(&self, key: &ARTKey) -> bool {
        if self.len() < key.len {
            return false;
        }
        for i in 0..key.len {
            if self.key_bytes[i] != key.get(i) {
                return false;
            }
        }
        true
    }

    /// Check if key_bytes is greater than [or equal to] the key.
    pub fn greater_than(&self, key: &ARTKey, equal: bool, nested_depth: u8) -> bool {
        let min_len = std::cmp::min(self.len(), key.len);
        for i in 0..min_len {
            if self.key_bytes[i] > key.get(i) {
                return true;
            } else if self.key_bytes[i] < key.get(i) {
                return false;
            }
        }

        // Returns true if current_key is greater than (or equal to) key.
        debug_assert!(self.len() >= nested_depth as usize);
        let this_len = self.len() - nested_depth as usize;
        if equal {
            this_len > key.len
        } else {
            this_len >= key.len
        }
    }
}

impl Default for IteratorKey {
    fn default() -> Self {
        Self::new()
    }
}

/// ART Iterator for tree traversal.
pub struct Iterator<'a> {
    /// Allocators for all node types.
    allocators: &'a [FixedSizeAllocator; ALLOCATOR_COUNT],
    /// Prefix byte capacity.
    prefix_count: usize,
    /// Stack of nodes from root to current position.
    nodes: Vec<IteratorEntry>,
    /// Current key leading to the top node on the stack.
    pub current_key: IteratorKey,
    /// Last visited leaf node.
    last_leaf: Node,
    /// Row ID bytes for nested leaves.
    row_id: [u8; ROW_ID_SIZE],
    /// Gate status (whether we passed a gate).
    status: GateStatus,
    /// Depth in a nested leaf.
    nested_depth: u8,
    /// Whether we entered a nested leaf to retrieve the next node.
    entered_nested_leaf: bool,
}

impl<'a> Iterator<'a> {
    /// Create a new iterator.
    pub fn new(allocators: &'a [FixedSizeAllocator; ALLOCATOR_COUNT], prefix_count: usize) -> Self {
        Self {
            allocators,
            prefix_count,
            nodes: Vec::new(),
            current_key: IteratorKey::new(),
            last_leaf: Node::empty(),
            row_id: [0u8; ROW_ID_SIZE],
            status: GateStatus::NotSet,
            nested_depth: 0,
            entered_nested_leaf: false,
        }
    }

    /// Get the nested depth.
    pub fn get_nested_depth(&self) -> u8 {
        self.nested_depth
    }

    /// Scan the tree, starting at the current position, ending at upper_bound.
    ///
    /// # Arguments
    /// * `upper_bound` - Upper bound key (empty means no upper bound)
    /// * `max_count` - Maximum number of row IDs to collect
    /// * `row_ids` - Output set of row IDs
    /// * `equal` - Whether to include the upper bound
    ///
    /// # Returns
    /// `true` if scan completed, `false` if max_count was exceeded
    pub fn scan(
        &mut self,
        upper_bound: Option<&ARTKey>,
        max_count: usize,
        row_ids: &mut BTreeSet<i64>,
        equal: bool,
    ) -> bool {
        loop {
            // Check upper bound
            if let Some(bound) = upper_bound {
                if (self.status == GateStatus::NotSet || self.entered_nested_leaf)
                    && self
                        .current_key
                        .greater_than(bound, equal, self.nested_depth)
                {
                    return true;
                }
            }

            // Process the current leaf
            match self.last_leaf.get_type() {
                NType::LeafInlined => {
                    if row_ids.len() + 1 > max_count {
                        return false;
                    }
                    row_ids.insert(self.last_leaf.get_row_id());
                }
                NType::Leaf => {
                    // Deprecated leaf - collect all row IDs
                    if !super::leaf::Leaf::deprecated_get_row_ids(
                        &self.allocators[1], // LEAF_ALLOC
                        &self.last_leaf,
                        row_ids,
                        max_count,
                    ) {
                        return false;
                    }
                }
                NType::Node7Leaf | NType::Node15Leaf | NType::Node256Leaf => {
                    // Nested leaf - iterate through all bytes
                    let mut byte = 0u8;
                    while self.get_next_byte_from_leaf(&self.last_leaf, &mut byte) {
                        if row_ids.len() + 1 > max_count {
                            return false;
                        }
                        self.row_id[ROW_ID_SIZE - 1] = byte;
                        let key = ARTKey::from_bytes_raw(&self.row_id);
                        row_ids.insert(key.get_row_id());
                        if byte == u8::MAX {
                            break;
                        }
                        byte += 1;
                    }
                }
                _ => {
                    // Invalid leaf type
                    return false;
                }
            }

            self.entered_nested_leaf = false;
            if !self.next() {
                return true;
            }
        }
    }

    /// Find the minimum (leftmost) leaf in the subtree.
    pub fn find_minimum(&mut self, node: &Node) {
        let mut current = *node;

        while current.has_metadata() {
            // Found the minimum
            if current.is_any_leaf() {
                self.last_leaf = current;
                return;
            }

            // We are passing a gate node
            if current.get_gate_status() == GateStatus::Set {
                debug_assert_eq!(self.status, GateStatus::NotSet);
                self.status = GateStatus::Set;
                self.entered_nested_leaf = true;
                self.nested_depth = 0;
            }

            // Traverse the prefix
            if current.get_type() == NType::Prefix {
                let prefix = Prefix::new(&self.allocators[0], current, self.prefix_count, false);
                let count = prefix.get_count(self.prefix_count);
                for i in 0..count as usize {
                    let byte = prefix.get_byte(i);
                    self.current_key.push(byte);
                    if self.status == GateStatus::Set {
                        self.row_id[self.nested_depth as usize] = byte;
                        self.nested_depth += 1;
                        debug_assert!((self.nested_depth as usize) < ROW_ID_SIZE);
                    }
                }
                self.nodes.push(IteratorEntry::new(current, 0));
                current = prefix.get_child();
                continue;
            }

            // Go to the leftmost entry in the current node
            let mut byte = 0u8;
            let next = self.get_next_child(&current, &mut byte);
            if next.is_none() {
                panic!("ART Iterator::find_minimum: No child found");
            }

            // Move to the leftmost node
            self.current_key.push(byte);
            if self.status == GateStatus::Set {
                self.row_id[self.nested_depth as usize] = byte;
                self.nested_depth += 1;
                debug_assert!((self.nested_depth as usize) < ROW_ID_SIZE);
            }
            self.nodes.push(IteratorEntry::new(current, byte));
            current = next.unwrap();
        }

        panic!("ART Iterator::find_minimum: Reached node without metadata");
    }

    /// Find the lower bound and add nodes to the stack.
    ///
    /// # Returns
    /// `true` if lower bound was found, `false` if it exceeds the maximum value
    pub fn lower_bound(&mut self, node: &Node, key: &ARTKey, equal: bool) -> bool {
        let mut current = *node;
        let mut depth = 0usize;

        while current.has_metadata() {
            // We found any leaf node, or a gate
            if current.is_any_leaf() || current.get_gate_status() == GateStatus::Set {
                debug_assert_eq!(self.status, GateStatus::NotSet);
                debug_assert_eq!(self.current_key.len(), key.len);

                if !equal && self.current_key.contains(key) {
                    return self.next();
                }

                if current.get_gate_status() == GateStatus::Set {
                    self.find_minimum(&current);
                } else {
                    self.last_leaf = current;
                }
                return true;
            }

            debug_assert_eq!(current.get_gate_status(), GateStatus::NotSet);

            if current.get_type() != NType::Prefix {
                let next_byte = key.get(depth);
                let mut byte = next_byte;
                let child = self.get_next_child(&current, &mut byte);

                // The key is greater than any key in this subtree
                if child.is_none() {
                    return self.next();
                }

                self.current_key.push(byte);
                self.nodes.push(IteratorEntry::new(current, byte));

                // We return the minimum because all keys are greater than the lower bound
                if byte > next_byte {
                    self.find_minimum(&child.unwrap());
                    return true;
                }

                // Move to the child and increment depth
                current = child.unwrap();
                depth += 1;
                continue;
            }

            // Push back all prefix bytes
            let prefix = Prefix::new(&self.allocators[0], current, self.prefix_count, false);
            let count = prefix.get_count(self.prefix_count) as usize;
            for i in 0..count {
                self.current_key.push(prefix.get_byte(i));
            }
            self.nodes.push(IteratorEntry::new(current, 0));

            // Compare the prefix bytes with the key bytes
            for i in 0..count {
                let prefix_byte = prefix.get_byte(i);
                let key_byte = key.get(depth + i);

                // Prefix byte is less than key byte - next node is the lower bound
                if prefix_byte < key_byte {
                    return self.next();
                }

                // Prefix byte is greater than key byte - minimum is the lower bound
                if prefix_byte > key_byte {
                    self.find_minimum(&prefix.get_child());
                    return true;
                }
            }

            // The prefix matches the key. Move to the child and update depth
            depth += count;
            current = prefix.get_child();
        }

        panic!("ART Iterator::lower_bound: Reached node without metadata");
    }

    /// Move to the next leaf in the ART.
    ///
    /// # Returns
    /// `true` if there is a next leaf, `false` otherwise
    fn next(&mut self) -> bool {
        while !self.nodes.is_empty() {
            // Get node info without holding mutable borrow
            let (node_type, node_copy, current_byte) = {
                let top = self.nodes.last().unwrap();
                (top.node.get_type(), top.node, top.byte)
            };

            if node_type == NType::Prefix {
                self.pop_node();
                continue;
            }

            if current_byte == u8::MAX {
                // No more children of this node
                self.pop_node();
                continue;
            }

            let mut byte = current_byte + 1;
            let next_node = self.get_next_child(&node_copy, &mut byte);

            if next_node.is_none() {
                // No more children of this node
                self.pop_node();
                continue;
            }

            // Update the byte in the entry
            if let Some(top) = self.nodes.last_mut() {
                top.byte = byte;
            }

            self.current_key.pop(1);
            self.current_key.push(byte);
            if self.status == GateStatus::Set {
                self.row_id[self.nested_depth as usize - 1] = byte;
            }

            self.find_minimum(&next_node.unwrap());
            return true;
        }
        false
    }

    /// Pop the top node from the stack.
    fn pop_node(&mut self) {
        let top = self.nodes.pop().unwrap();
        let gate_status = top.node.get_gate_status();

        // Pop the byte and the node
        if top.node.get_type() != NType::Prefix {
            self.current_key.pop(1);
            if self.status == GateStatus::Set {
                self.nested_depth -= 1;
                debug_assert!((self.nested_depth as usize) < ROW_ID_SIZE);
            }
        } else {
            // Pop all prefix bytes
            let prefix = Prefix::new(&self.allocators[0], top.node, self.prefix_count, false);
            let prefix_byte_count = prefix.get_count(self.prefix_count) as usize;
            self.current_key.pop(prefix_byte_count);

            if self.status == GateStatus::Set {
                self.nested_depth -= prefix_byte_count as u8;
                debug_assert!((self.nested_depth as usize) < ROW_ID_SIZE);
            }
        }

        // We are popping a gate node
        if gate_status == GateStatus::Set {
            debug_assert_eq!(self.status, GateStatus::Set);
            self.status = GateStatus::NotSet;
        }
    }

    /// Get the next child >= byte from a node.
    fn get_next_child(&self, node: &Node, byte: &mut u8) -> Option<Node> {
        match node.get_type() {
            NType::Node4 => {
                let handle = Node4::get(&self.allocators[2], *node);
                handle.get_next_child(byte).copied()
            }
            NType::Node16 => {
                let handle = Node16::get(&self.allocators[3], *node);
                handle.get_next_child(byte).copied()
            }
            NType::Node48 => {
                let handle = Node48::get(&self.allocators[4], *node);
                handle.get_next_child(byte).copied()
            }
            NType::Node256 => {
                let handle = Node256::get(&self.allocators[5], *node);
                handle.get_next_child(byte).copied()
            }
            _ => None,
        }
    }

    /// Get the next byte from a leaf node.
    fn get_next_byte_from_leaf(&self, node: &Node, byte: &mut u8) -> bool {
        match node.get_type() {
            NType::Node7Leaf => {
                let handle = Node7Leaf::get(&self.allocators[6], *node);
                if let Some(b) = handle.get_next_byte(*byte) {
                    *byte = b;
                    true
                } else {
                    false
                }
            }
            NType::Node15Leaf => {
                let handle = Node15Leaf::get(&self.allocators[7], *node);
                if let Some(b) = handle.get_next_byte(*byte) {
                    *byte = b;
                    true
                } else {
                    false
                }
            }
            NType::Node256Leaf => {
                let handle = Node256Leaf::get(&self.allocators[8], *node);
                if let Some(b) = handle.get_next_byte(*byte) {
                    *byte = b;
                    true
                } else {
                    false
                }
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iterator_key_new() {
        let key = IteratorKey::new();
        assert!(key.is_empty());
        assert_eq!(key.len(), 0);
    }

    #[test]
    fn test_iterator_key_push_pop() {
        let mut key = IteratorKey::new();
        key.push(1);
        key.push(2);
        key.push(3);

        assert_eq!(key.len(), 3);
        assert_eq!(key.get(0), 1);
        assert_eq!(key.get(1), 2);
        assert_eq!(key.get(2), 3);

        key.pop(2);
        assert_eq!(key.len(), 1);
        assert_eq!(key.get(0), 1);
    }

    #[test]
    fn test_iterator_entry() {
        let node = Node::new(1, 100);
        let entry = IteratorEntry::new(node, 42);

        assert_eq!(entry.node.get_buffer_id(), 1);
        assert_eq!(entry.byte, 42);
    }
}
