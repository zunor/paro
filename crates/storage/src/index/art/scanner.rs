// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # ART Scanner - Tree traversal for processing all nodes
//!
//! ## Design
//!
//! The ARTScanner provides tree traversal for processing all nodes:
//! - Uses a stack-based approach for depth-first traversal
//! - Supports two handling modes: EMPLACE (process on push) and POP (process on pop)
//! - Handles all node types including PREFIX and internal nodes

use super::internal_node::{Node16, Node256, Node4, Node48};
use super::node::{NType, Node, ALLOCATOR_COUNT};
use super::prefix::Prefix;
use crate::index::fixed_size_allocator::FixedSizeAllocator;

/// Result of handling a node during scanning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ARTHandlingResult {
    /// Continue traversing children
    Continue,
    /// Skip this node's children
    Skip,
    /// No specific action (used for POP handling)
    None,
}

/// Handling mode for the scanner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ARTScanHandling {
    /// Process nodes when pushing onto the stack
    Emplace,
    /// Process nodes when popping from the stack
    Pop,
}

/// Entry in the scanner stack.
struct ScannerEntry {
    /// The node at this level.
    node: Node,
    /// Whether this node's children have been processed.
    exhausted: bool,
}

impl ScannerEntry {
    fn new(node: Node) -> Self {
        Self {
            node,
            exhausted: false,
        }
    }
}

/// ARTScanner scans the entire ART and processes each node.
///
/// The scanner uses a stack-based depth-first traversal to visit all nodes
/// in the tree. It supports two handling modes:
/// - EMPLACE: Process nodes when they are first encountered
/// - POP: Process nodes after all their children have been processed
pub struct ARTScanner<'a> {
    /// Allocators for all node types.
    allocators: &'a [FixedSizeAllocator; ALLOCATOR_COUNT],
    /// Prefix byte capacity.
    prefix_count: usize,
    /// Stack of nodes to process.
    stack: Vec<ScannerEntry>,
    /// Handling mode.
    handling: ARTScanHandling,
}

impl<'a> ARTScanner<'a> {
    /// Create a new scanner with the given handling mode.
    pub fn new(
        allocators: &'a [FixedSizeAllocator; ALLOCATOR_COUNT],
        prefix_count: usize,
        handling: ARTScanHandling,
    ) -> Self {
        Self {
            allocators,
            prefix_count,
            stack: Vec::new(),
            handling,
        }
    }

    /// Initialize the scanner with a root node.
    pub fn init<F>(&mut self, root: &mut Node, handler: &mut F)
    where
        F: FnMut(&mut Node) -> ARTHandlingResult,
    {
        self.emplace(root, handler);
    }

    /// Scan the tree, calling the handler for each node.
    pub fn scan<F>(&mut self, handler: &mut F)
    where
        F: FnMut(&mut Node) -> ARTHandlingResult,
    {
        while !self.stack.is_empty() {
            let entry_idx = self.stack.len() - 1;

            if self.stack[entry_idx].exhausted {
                // Pop and optionally process
                let entry = self.stack.pop().unwrap();
                if self.handling == ARTScanHandling::Pop {
                    let mut node = entry.node;
                    handler(&mut node);
                }
                continue;
            }

            // Mark as exhausted before processing children
            self.stack[entry_idx].exhausted = true;
            let node = self.stack[entry_idx].node;

            let ntype = node.get_type();
            match ntype {
                NType::LeafInlined
                | NType::Leaf
                | NType::Node7Leaf
                | NType::Node15Leaf
                | NType::Node256Leaf => {
                    // Leaf nodes have no children to process
                }
                NType::Prefix => {
                    let prefix = Prefix::new(&self.allocators[0], node, self.prefix_count, false);
                    let mut child = prefix.get_child();
                    self.emplace(&mut child, handler);
                }
                NType::Node4 => {
                    self.iterate_children_node4(node, handler);
                }
                NType::Node16 => {
                    self.iterate_children_node16(node, handler);
                }
                NType::Node48 => {
                    self.iterate_children_node48(node, handler);
                }
                NType::Node256 => {
                    self.iterate_children_node256(node, handler);
                }
            }
        }
    }

    /// Push a node onto the stack, optionally processing it.
    fn emplace<F>(&mut self, node: &mut Node, handler: &mut F)
    where
        F: FnMut(&mut Node) -> ARTHandlingResult,
    {
        if !node.has_metadata() {
            return;
        }

        if self.handling == ARTScanHandling::Emplace {
            let result = handler(node);
            if result == ARTHandlingResult::Skip {
                return;
            }
        }

        self.stack.push(ScannerEntry::new(*node));
    }

    /// Iterate children of a Node4.
    fn iterate_children_node4<F>(&mut self, node: Node, handler: &mut F)
    where
        F: FnMut(&mut Node) -> ARTHandlingResult,
    {
        let handle = Node4::get(&self.allocators[2], node);
        let count = handle.get_count() as usize;

        // Collect children first to avoid borrow issues
        let mut children = Vec::with_capacity(count);
        for i in 0..count {
            let child = handle.get_child_at(i as u8);
            if child.has_metadata() {
                children.push(child);
            }
        }

        // Process children
        for mut child in children {
            self.emplace(&mut child, handler);
        }
    }

    /// Iterate children of a Node16.
    fn iterate_children_node16<F>(&mut self, node: Node, handler: &mut F)
    where
        F: FnMut(&mut Node) -> ARTHandlingResult,
    {
        let handle = Node16::get(&self.allocators[3], node);
        let count = handle.get_count() as usize;

        let mut children = Vec::with_capacity(count);
        for i in 0..count {
            let child = handle.get_child_at(i as u8);
            if child.has_metadata() {
                children.push(child);
            }
        }

        for mut child in children {
            self.emplace(&mut child, handler);
        }
    }

    /// Iterate children of a Node48.
    fn iterate_children_node48<F>(&mut self, node: Node, handler: &mut F)
    where
        F: FnMut(&mut Node) -> ARTHandlingResult,
    {
        let handle = Node48::get(&self.allocators[4], node);

        let mut children = Vec::new();
        for byte in 0..=255u8 {
            if let Some(child) = handle.get_child(byte) {
                children.push(*child);
            }
        }

        for mut child in children {
            self.emplace(&mut child, handler);
        }
    }

    /// Iterate children of a Node256.
    fn iterate_children_node256<F>(&mut self, node: Node, handler: &mut F)
    where
        F: FnMut(&mut Node) -> ARTHandlingResult,
    {
        let handle = Node256::get(&self.allocators[5], node);

        let mut children = Vec::new();
        for byte in 0..=255u8 {
            if let Some(child) = handle.get_child(byte) {
                children.push(*child);
            }
        }

        for mut child in children {
            self.emplace(&mut child, handler);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handling_result() {
        assert_ne!(ARTHandlingResult::Continue, ARTHandlingResult::Skip);
        assert_ne!(ARTHandlingResult::Skip, ARTHandlingResult::None);
    }

    #[test]
    fn test_scan_handling() {
        assert_ne!(ARTScanHandling::Emplace, ARTScanHandling::Pop);
    }
}
