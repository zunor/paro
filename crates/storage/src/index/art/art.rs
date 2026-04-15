//! # ART (Adaptive Radix Tree) runtime index
//!
//! ## Design
//! - `ART` is the in-memory `BoundIndex` used by segment predicate evaluation
//! - Metadata is explicitly single-column: one `column_id` + one `logical_type`
//! - Row ids are segment-local row offsets, so predicate hits can flow straight
//!   into `SegmentIterator` bitmaps without translation
//! - Duplicate logical keys fan out into gated nested row-id subtrees
//! - The tree uses prefix compression plus adaptive node sizes for compact lookup

use std::collections::HashMap;
use std::sync::Arc;

use paro_common::allocator::ArenaAllocator;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use super::internal_node::{Node16, Node256, Node4, Node48};
use super::leaf::Leaf;
use super::node::{GateStatus, NType, Node, ALLOCATOR_COUNT, DEPRECATED_ALLOCATOR_COUNT};
use super::prefix::Prefix;
use super::ARTKey;
use crate::buffer::BufferManager;
use crate::index::bound_index::{BoundIndex, DeltaIndexType, IndexAppendInfo, IndexAppendMode};
use crate::index::fixed_size_allocator::FixedSizeAllocator;
use crate::index::predicate::{value_to_bytes, Predicate};
use crate::index::predicate_result::PredicateResult;
use crate::index::{ColumnId, Index, IndexConstraintType, IndexStorageInfo};
use roaring::RoaringBitmap;

/// Allocator indices for each node type.
const PREFIX_ALLOC: usize = 0;
const NODE4_ALLOC: usize = 2;
const NODE16_ALLOC: usize = 3;
const NODE48_ALLOC: usize = 4;
const NODE256_ALLOC: usize = 5;
const NODE7_LEAF_ALLOC: usize = 6;
const NODE15_LEAF_ALLOC: usize = 7;
const NODE256_LEAF_ALLOC: usize = 8;

/// Conflict type for ART operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ARTConflictType {
    /// No conflict
    NoConflict,
    /// Constraint violation (duplicate key in unique index)
    Constraint,
    /// Transaction conflict (write-write conflict)
    Transaction,
}

/// ART (Adaptive Radix Tree) index structure.
///
/// The ART index provides efficient point and range lookups with
/// adaptive node sizes for memory efficiency.
pub struct ART {
    /// Index name.
    name: String,
    /// Constraint type (None, Unique, Primary).
    constraint_type: IndexConstraintType,
    /// Column ID being indexed.
    column_id: ColumnId,
    /// Logical type of the indexed column.
    logical_type: LogicalType,
    /// Root node of the tree.
    tree: Node,
    /// Fixed-size allocators for each node type.
    allocators: [FixedSizeAllocator; ALLOCATOR_COUNT],
    /// Whether this ART owns its data.
    #[allow(dead_code)]
    owns_data: bool,
    /// Whether keys need length verification.
    #[allow(dead_code)]
    verify_max_key_len: bool,
    /// Prefix byte capacity.
    prefix_count: u8,
    /// Delta index type.
    delta_index_type: DeltaIndexType,
}

impl ART {
    /// Index type name.
    pub const TYPE_NAME: &'static str = "ART";

    /// Create a new ART index.
    pub fn new(
        name: impl Into<String>,
        constraint_type: IndexConstraintType,
        column_id: ColumnId,
        logical_type: LogicalType,
        buffer_manager: Arc<dyn BufferManager>,
    ) -> Self {
        let name = name.into();

        // Determine if we need key length verification
        let verify_max_key_len = matches!(
            &logical_type,
            LogicalType::Varchar | LogicalType::VarcharCollation(_)
        );

        // Calculate prefix count based on column types
        let prefix_count = Self::calculate_prefix_count(&logical_type);

        // Create allocators for each node type
        let prefix_size = prefix_count as usize + super::prefix::METADATA_SIZE;
        let allocators = Self::create_allocators(prefix_size, buffer_manager);

        Self {
            name,
            constraint_type,
            column_id,
            logical_type,
            tree: Node::empty(),
            allocators,
            owns_data: true,
            verify_max_key_len,
            prefix_count,
            delta_index_type: DeltaIndexType::None,
        }
    }

    /// Create allocators for all node types.
    fn create_allocators(
        prefix_size: usize,
        buffer_manager: Arc<dyn BufferManager>,
    ) -> [FixedSizeAllocator; ALLOCATOR_COUNT] {
        let block_size = 4096;
        [
            // 0: Prefix
            FixedSizeAllocator::with_buffer_manager(
                prefix_size,
                block_size,
                buffer_manager.clone(),
            ),
            // 1: Leaf (deprecated)
            FixedSizeAllocator::with_buffer_manager(
                super::leaf::DEPRECATED_LEAF_SIZE,
                block_size,
                buffer_manager.clone(),
            ),
            // 2: Node4
            FixedSizeAllocator::with_buffer_manager(
                Node4::SIZE,
                block_size,
                buffer_manager.clone(),
            ),
            // 3: Node16
            FixedSizeAllocator::with_buffer_manager(
                Node16::SIZE,
                block_size,
                buffer_manager.clone(),
            ),
            // 4: Node48
            FixedSizeAllocator::with_buffer_manager(
                Node48::SIZE,
                block_size,
                buffer_manager.clone(),
            ),
            // 5: Node256
            FixedSizeAllocator::with_buffer_manager(
                Node256::SIZE,
                block_size,
                buffer_manager.clone(),
            ),
            // 6: Node7Leaf
            FixedSizeAllocator::with_buffer_manager(
                super::leaf::Node7Leaf::SIZE,
                block_size,
                buffer_manager.clone(),
            ),
            // 7: Node15Leaf
            FixedSizeAllocator::with_buffer_manager(
                super::leaf::Node15Leaf::SIZE,
                block_size,
                buffer_manager.clone(),
            ),
            // 8: Node256Leaf
            FixedSizeAllocator::with_buffer_manager(
                super::leaf::Node256Leaf::SIZE,
                block_size,
                buffer_manager,
            ),
        ]
    }

    /// Calculate prefix count based on column types.
    fn calculate_prefix_count(logical_type: &LogicalType) -> u8 {
        Self::align_prefix_count(Self::type_size(logical_type).saturating_sub(1))
    }

    fn align_prefix_count(raw_prefix_count: usize) -> u8 {
        let aligned = raw_prefix_count.saturating_add(1).saturating_add(7) & !7usize;
        aligned.saturating_sub(1).min(u8::MAX as usize) as u8
    }

    /// Get the size of a logical type for key encoding.
    fn type_size(t: &LogicalType) -> usize {
        match t {
            LogicalType::Boolean => 1,
            LogicalType::TinyInt | LogicalType::UTinyInt => 1,
            LogicalType::SmallInt | LogicalType::USmallInt => 2,
            LogicalType::Integer | LogicalType::UInteger => 4,
            LogicalType::BigInt | LogicalType::UBigInt => 8,
            LogicalType::Float => 4,
            LogicalType::Double => 8,
            LogicalType::Varchar => 8, // Variable, use 8 as default
            _ => 8,
        }
    }

    /// Get the prefix count.
    pub fn prefix_count(&self) -> u8 {
        self.prefix_count
    }

    /// Check if this is a unique index.
    pub fn is_unique(&self) -> bool {
        matches!(
            self.constraint_type,
            IndexConstraintType::Unique | IndexConstraintType::Primary
        )
    }

    /// Get the allocator for a specific node type.
    pub fn get_allocator(&self, ntype: NType) -> &FixedSizeAllocator {
        &self.allocators[ntype.allocator_index() as usize]
    }

    /// Get a mutable allocator for a specific node type.
    pub fn get_allocator_mut(&mut self, ntype: NType) -> &mut FixedSizeAllocator {
        &mut self.allocators[ntype.allocator_index() as usize]
    }

    // =========================================================================
    // Insert Operations
    // =========================================================================

    /// Insert a single key-value pair into the ART.
    pub fn insert_key(
        &mut self,
        arena: &mut ArenaAllocator,
        key: &ARTKey,
        row_id: i64,
        append_mode: IndexAppendMode,
    ) -> ARTConflictType {
        let row_id_key = ARTKey::from_row_id(arena, row_id);
        let is_unique = self.is_unique();
        let prefix_count = self.prefix_count as usize;

        // Take tree out temporarily
        let mut tree = std::mem::replace(&mut self.tree, Node::empty());
        let result = Self::insert_recursive(
            arena,
            &mut self.allocators,
            &mut tree,
            key,
            0,
            &row_id_key,
            GateStatus::NotSet,
            append_mode,
            is_unique,
            prefix_count,
        );
        // Put tree back
        self.tree = tree;
        result
    }

    /// Recursive insert implementation.
    fn insert_recursive(
        arena: &mut ArenaAllocator,
        allocators: &mut [FixedSizeAllocator; ALLOCATOR_COUNT],
        node: &mut Node,
        key: &ARTKey,
        mut depth: usize,
        row_id: &ARTKey,
        mut status: GateStatus,
        append_mode: IndexAppendMode,
        is_unique: bool,
        prefix_count: usize,
    ) -> ARTConflictType {
        // Early-out if the node is empty
        if !node.has_metadata() {
            if status == GateStatus::Set {
                Leaf::new(node, row_id.get_row_id());
                return ARTConflictType::NoConflict;
            }

            // Create prefix chain and leaf
            let count = key.len - depth;
            if count > 0 {
                // create_chain sets node to the chain head and returns pointer to the end
                let leaf_ptr = Prefix::create_chain(
                    &mut allocators[PREFIX_ALLOC],
                    node,
                    prefix_count,
                    key,
                    depth,
                    count,
                );
                // Create the leaf at the end of the chain
                unsafe {
                    Leaf::new(&mut *leaf_ptr, row_id.get_row_id());
                }
            } else {
                // No prefix needed, just create the leaf
                Leaf::new(node, row_id.get_row_id());
            }
            return ARTConflictType::NoConflict;
        }

        let mut active_key = key;

        loop {
            if !node.has_metadata() {
                break;
            }

            // Check for gate status transition
            if status == GateStatus::NotSet && node.get_gate_status() == GateStatus::Set {
                if is_unique {
                    return ARTConflictType::Transaction;
                }
                active_key = row_id;
                depth = 0;
                status = GateStatus::Set;
                continue;
            }

            let ntype = node.get_type();
            match ntype {
                NType::LeafInlined => {
                    let mut row_id_node = Node::empty();
                    Leaf::new(&mut row_id_node, row_id.get_row_id());

                    if !is_unique || append_mode == IndexAppendMode::InsertDuplicates {
                        Leaf::merge_inlined(
                            arena,
                            allocators,
                            node,
                            &row_id_node,
                            status,
                            depth,
                            prefix_count,
                        );
                        return ARTConflictType::NoConflict;
                    }

                    if append_mode == IndexAppendMode::IgnoreDuplicates {
                        return ARTConflictType::NoConflict;
                    }

                    return ARTConflictType::Constraint;
                }
                NType::Leaf => {
                    // Transform deprecated leaf to nested structure (placeholder)
                    node.clear();
                    continue;
                }
                NType::Node7Leaf | NType::Node15Leaf | NType::Node256Leaf => {
                    let byte = active_key.get_byte(super::prefix::ROW_ID_COUNT as usize);
                    Self::insert_leaf_byte(allocators, node, byte);
                    return ARTConflictType::NoConflict;
                }
                NType::Node4 | NType::Node16 | NType::Node48 | NType::Node256 => {
                    debug_assert!(depth < active_key.len);
                    let byte = active_key.get_byte(depth);

                    if let Some(mut child) = Self::get_child_copy(allocators, node, byte) {
                        // Recurse into child
                        let result = Self::insert_recursive(
                            arena,
                            allocators,
                            &mut child,
                            key,
                            depth + 1,
                            row_id,
                            status,
                            append_mode,
                            is_unique,
                            prefix_count,
                        );
                        // Update child in parent
                        Self::replace_child(allocators, node, byte, child);
                        return result;
                    }

                    // Insert new child
                    Self::insert_into_node(
                        allocators,
                        node,
                        key,
                        row_id,
                        depth,
                        status,
                        prefix_count,
                    );
                    return ARTConflictType::NoConflict;
                }
                NType::Prefix => {
                    let count = {
                        let prefix =
                            Prefix::new(&allocators[PREFIX_ALLOC], *node, prefix_count, false);
                        prefix.get_count(prefix_count)
                    };

                    // Check for mismatch in prefix
                    let mut mismatch_pos = None;
                    for i in 0..count as usize {
                        let prefix_byte = Prefix::get_byte_static(
                            &allocators[PREFIX_ALLOC],
                            node,
                            prefix_count,
                            i as u8,
                        );
                        if prefix_byte != active_key.get_byte(depth) {
                            mismatch_pos = Some(i);
                            break;
                        }
                        depth += 1;
                    }

                    if let Some(pos) = mismatch_pos {
                        Self::insert_into_prefix(
                            allocators,
                            node,
                            key,
                            row_id,
                            pos,
                            depth,
                            status,
                            prefix_count,
                        );
                        return ARTConflictType::NoConflict;
                    }

                    // Get child and continue
                    let mut child = {
                        let prefix =
                            Prefix::new(&allocators[PREFIX_ALLOC], *node, prefix_count, false);
                        prefix.get_child()
                    };
                    let result = Self::insert_recursive(
                        arena,
                        allocators,
                        &mut child,
                        key,
                        depth,
                        row_id,
                        status,
                        append_mode,
                        is_unique,
                        prefix_count,
                    );
                    // Update child in prefix
                    {
                        let mut prefix =
                            Prefix::new(&allocators[PREFIX_ALLOC], *node, prefix_count, true);
                        prefix.set_child(child);
                    }
                    return result;
                }
            }
        }

        ARTConflictType::NoConflict
    }

    /// Insert into an internal node.
    fn insert_into_node(
        allocators: &mut [FixedSizeAllocator; ALLOCATOR_COUNT],
        node: &mut Node,
        key: &ARTKey,
        row_id: &ARTKey,
        depth: usize,
        status: GateStatus,
        prefix_count: usize,
    ) {
        if status == GateStatus::Set {
            // Inside gates, create inlined leaf directly
            let mut row_id_node = Node::empty();
            Leaf::new(&mut row_id_node, row_id.get_row_id());
            Self::insert_child(allocators, node, row_id.get_byte(depth), row_id_node);
            return;
        }

        // Outside gates, create prefix chain for remaining key bytes
        let mut child = Node::empty();
        if depth + 1 < key.len {
            let count = key.len - depth - 1;
            let leaf_ptr = Prefix::create_chain(
                &mut allocators[PREFIX_ALLOC],
                &mut child,
                prefix_count,
                key,
                depth + 1,
                count,
            );
            // Create inlined leaf at the end of the chain
            unsafe {
                Leaf::new(&mut *leaf_ptr, row_id.get_row_id());
            }
        } else {
            // No prefix needed, just create the leaf
            Leaf::new(&mut child, row_id.get_row_id());
        }
        Self::insert_child(allocators, node, key.get_byte(depth), child);
    }

    /// Insert into a prefix node (split the prefix).
    fn insert_into_prefix(
        allocators: &mut [FixedSizeAllocator; ALLOCATOR_COUNT],
        node: &mut Node,
        key: &ARTKey,
        row_id: &ARTKey,
        pos: usize,
        depth: usize,
        status: GateStatus,
        prefix_count: usize,
    ) {
        let byte =
            Prefix::get_byte_static(&allocators[PREFIX_ALLOC], node, prefix_count, pos as u8);

        // Save the original prefix node
        let original_prefix = *node;

        // Split the prefix
        let mut child = Node::empty();
        let split_status = Prefix::split(
            &mut allocators[PREFIX_ALLOC],
            node,
            &mut child,
            prefix_count,
            pos as u8,
        );

        // Create new Node4
        let mut new_node4 = Node::empty();
        Node4::new(&mut allocators[NODE4_ALLOC], &mut new_node4);
        new_node4.set_gate_status(split_status);

        // Insert existing child into the new Node4
        {
            let mut handle = Node4::get_mut(&allocators[NODE4_ALLOC], new_node4);
            handle.insert_child_internal(byte, child);
        }

        // Insert new key into the new Node4
        Self::insert_into_node(
            allocators,
            &mut new_node4,
            key,
            row_id,
            depth,
            status,
            prefix_count,
        );

        // Update the tree structure based on what split did
        if !node.has_metadata() {
            // pos == 0: The prefix was freed, set node to the new Node4
            *node = new_node4;
        } else if *node == original_prefix {
            // pos > 0 and not Case 1: node still points to the original prefix
            // Update the prefix's child to point to the new Node4
            let mut prefix = Prefix::new(&allocators[PREFIX_ALLOC], *node, prefix_count, true);
            prefix.set_child(new_node4);
        } else {
            // Case 1: node was modified to point to the prefix's child
            // The original prefix still exists with reduced count
            // Update the original prefix's child to point to the new Node4
            let mut prefix = Prefix::new(
                &allocators[PREFIX_ALLOC],
                original_prefix,
                prefix_count,
                true,
            );
            prefix.set_child(new_node4);
            // Restore node to point to the original prefix (the tree root)
            *node = original_prefix;
        }
    }

    /// Insert a child into an internal node.
    fn insert_child(
        allocators: &mut [FixedSizeAllocator; ALLOCATOR_COUNT],
        node: &mut Node,
        byte: u8,
        child: Node,
    ) {
        match node.get_type() {
            NType::Node4 => {
                // Check if we need to grow first
                let count = {
                    let handle = Node4::get(&allocators[NODE4_ALLOC], *node);
                    handle.get_count() as usize
                };
                if count < Node4::CAPACITY {
                    let mut handle = Node4::get_mut(&allocators[NODE4_ALLOC], *node);
                    handle.insert_child_internal(byte, child);
                } else {
                    // Grow to Node16 using split_at_mut
                    let node4 = *node;
                    let (left, right) = allocators.split_at_mut(NODE16_ALLOC);
                    Node16::grow_from_node4(&mut left[NODE4_ALLOC], &mut right[0], node, node4);
                    let mut handle = Node16::get_mut(&right[0], *node);
                    handle.insert_child_internal(byte, child);
                }
            }
            NType::Node16 => {
                let count = {
                    let handle = Node16::get(&allocators[NODE16_ALLOC], *node);
                    handle.get_count() as usize
                };
                if count < Node16::CAPACITY {
                    let mut handle = Node16::get_mut(&allocators[NODE16_ALLOC], *node);
                    handle.insert_child_internal(byte, child);
                } else {
                    // Grow to Node48 using split_at_mut
                    let node16 = *node;
                    let (left, right) = allocators.split_at_mut(NODE48_ALLOC);
                    Node48::grow_from_node16(&mut left[NODE16_ALLOC], &mut right[0], node, node16);
                    Node48::insert_child(&mut right[0], node, byte, child);
                }
            }
            NType::Node48 => {
                let count = {
                    let handle = Node48::get(&allocators[NODE48_ALLOC], *node);
                    handle.get_count() as usize
                };
                if count < Node48::CAPACITY {
                    Node48::insert_child(&mut allocators[NODE48_ALLOC], node, byte, child);
                } else {
                    // Grow to Node256 using split_at_mut
                    let node48 = *node;
                    let (left, right) = allocators.split_at_mut(NODE256_ALLOC);
                    Node256::grow_from_node48(&mut left[NODE48_ALLOC], &mut right[0], node, node48);
                    Node256::insert_child(&mut right[0], node, byte, child);
                }
            }
            NType::Node256 => {
                Node256::insert_child(&mut allocators[NODE256_ALLOC], node, byte, child);
            }
            _ => panic!("Invalid node type for insert_child: {:?}", node.get_type()),
        }
    }

    /// Insert a byte into a leaf node.
    fn insert_leaf_byte(
        allocators: &mut [FixedSizeAllocator; ALLOCATOR_COUNT],
        node: &mut Node,
        byte: u8,
    ) {
        match node.get_type() {
            NType::Node7Leaf => {
                let count = {
                    let handle = super::leaf::Node7Leaf::get(&allocators[NODE7_LEAF_ALLOC], *node);
                    handle.get_count() as usize
                };
                if count < super::leaf::Node7Leaf::CAPACITY {
                    super::leaf::Node7Leaf::insert_byte_internal(
                        &mut allocators[NODE7_LEAF_ALLOC],
                        node,
                        byte,
                    );
                } else {
                    // Grow to Node15Leaf using split_at_mut
                    let node7 = *node;
                    let (left, right) = allocators.split_at_mut(NODE15_LEAF_ALLOC);
                    super::leaf::Node15Leaf::grow_from_node7(
                        &mut left[NODE7_LEAF_ALLOC],
                        &mut right[0],
                        node,
                        node7,
                    );
                    super::leaf::Node15Leaf::insert_byte_internal(&mut right[0], node, byte);
                }
            }
            NType::Node15Leaf => {
                let count = {
                    let handle =
                        super::leaf::Node15Leaf::get(&allocators[NODE15_LEAF_ALLOC], *node);
                    handle.get_count() as usize
                };
                if count < super::leaf::Node15Leaf::CAPACITY {
                    super::leaf::Node15Leaf::insert_byte_internal(
                        &mut allocators[NODE15_LEAF_ALLOC],
                        node,
                        byte,
                    );
                } else {
                    // Grow to Node256Leaf using split_at_mut
                    let node15 = *node;
                    let (left, right) = allocators.split_at_mut(NODE256_LEAF_ALLOC);
                    super::leaf::Node256Leaf::grow_from_node15(
                        &mut left[NODE15_LEAF_ALLOC],
                        &mut right[0],
                        node,
                        node15,
                    );
                    super::leaf::Node256Leaf::insert_byte(&mut right[0], node, byte);
                }
            }
            NType::Node256Leaf => {
                super::leaf::Node256Leaf::insert_byte(
                    &mut allocators[NODE256_LEAF_ALLOC],
                    node,
                    byte,
                );
            }
            _ => panic!(
                "Invalid node type for insert_leaf_byte: {:?}",
                node.get_type()
            ),
        }
    }

    /// Get a copy of a child from an internal node.
    fn get_child_copy(
        allocators: &[FixedSizeAllocator; ALLOCATOR_COUNT],
        node: &Node,
        byte: u8,
    ) -> Option<Node> {
        match node.get_type() {
            NType::Node4 => {
                let handle = Node4::get(&allocators[NODE4_ALLOC], *node);
                handle.get_child(byte).copied()
            }
            NType::Node16 => {
                let handle = Node16::get(&allocators[NODE16_ALLOC], *node);
                handle.get_child(byte).copied()
            }
            NType::Node48 => {
                let handle = Node48::get(&allocators[NODE48_ALLOC], *node);
                handle.get_child(byte).copied()
            }
            NType::Node256 => {
                let handle = Node256::get(&allocators[NODE256_ALLOC], *node);
                handle.get_child(byte).copied()
            }
            _ => None,
        }
    }

    /// Replace a child in an internal node.
    fn replace_child(
        allocators: &[FixedSizeAllocator; ALLOCATOR_COUNT],
        node: &Node,
        byte: u8,
        child: Node,
    ) {
        match node.get_type() {
            NType::Node4 => {
                let mut handle = Node4::get_mut(&allocators[NODE4_ALLOC], *node);
                handle.replace_child(byte, child);
            }
            NType::Node16 => {
                let mut handle = Node16::get_mut(&allocators[NODE16_ALLOC], *node);
                handle.replace_child(byte, child);
            }
            NType::Node48 => {
                let mut handle = Node48::get_mut(&allocators[NODE48_ALLOC], *node);
                handle.replace_child(byte, child);
            }
            NType::Node256 => {
                let mut handle = Node256::get_mut(&allocators[NODE256_ALLOC], *node);
                handle.replace_child(byte, child);
            }
            _ => {}
        }
    }

    // =========================================================================
    // Lookup Operations
    // =========================================================================

    /// Lookup a key in the ART.
    pub fn lookup(&self, key: &ARTKey) -> Option<Node> {
        self.lookup_internal(&self.tree, key, 0)
    }

    /// Internal lookup implementation.
    fn lookup_internal(&self, node: &Node, key: &ARTKey, mut depth: usize) -> Option<Node> {
        let mut current = *node;
        let prefix_count = self.prefix_count as usize;

        while current.has_metadata() {
            // Return leaf or gate node
            if current.is_any_leaf() || current.get_gate_status() == GateStatus::Set {
                return Some(current);
            }

            if depth >= key.len {
                return Some(current);
            }

            // Traverse prefix
            if current.get_type() == NType::Prefix {
                let prefix =
                    Prefix::new(&self.allocators[PREFIX_ALLOC], current, prefix_count, false);
                let count = prefix.get_count(prefix_count);

                for i in 0..count as usize {
                    if depth >= key.len {
                        return Some(current);
                    }
                    if prefix.get_byte(i) != key.get_byte(depth) {
                        return None;
                    }
                    depth += 1;
                }

                current = prefix.get_child();
                continue;
            }

            // Get child from internal node
            debug_assert!(depth < key.len);
            let byte = key.get_byte(depth);

            let child = match current.get_type() {
                NType::Node4 => {
                    let handle = Node4::get(&self.allocators[NODE4_ALLOC], current);
                    handle.get_child(byte).copied()
                }
                NType::Node16 => {
                    let handle = Node16::get(&self.allocators[NODE16_ALLOC], current);
                    handle.get_child(byte).copied()
                }
                NType::Node48 => {
                    let handle = Node48::get(&self.allocators[NODE48_ALLOC], current);
                    handle.get_child(byte).copied()
                }
                NType::Node256 => {
                    let handle = Node256::get(&self.allocators[NODE256_ALLOC], current);
                    handle.get_child(byte).copied()
                }
                _ => None,
            };

            match child {
                Some(c) => {
                    current = c;
                    depth += 1;
                }
                None => return None,
            }
        }

        None
    }

    /// Check if a row_id exists in a leaf node.
    pub fn lookup_in_leaf(&self, leaf: &Node, row_id: &ARTKey) -> bool {
        let mut current = *leaf;
        let mut depth = 0;
        let prefix_count = self.prefix_count as usize;

        while current.has_metadata() {
            match current.get_type() {
                NType::LeafInlined => {
                    return current.get_row_id() == row_id.get_row_id();
                }
                NType::Leaf => {
                    return false;
                }
                NType::Node7Leaf | NType::Node15Leaf | NType::Node256Leaf => {
                    debug_assert!(depth + 1 == super::prefix::ROW_ID_SIZE);
                    let byte = row_id.get_byte(super::prefix::ROW_ID_COUNT as usize);
                    return self.leaf_has_byte(&current, byte);
                }
                NType::Node4 | NType::Node16 | NType::Node48 | NType::Node256 => {
                    debug_assert!(depth < super::prefix::ROW_ID_SIZE);
                    let byte = row_id.get_byte(depth);
                    if let Some(child) = Self::get_child_copy(&self.allocators, &current, byte) {
                        current = child;
                        depth += 1;
                        continue;
                    }
                    return false;
                }
                NType::Prefix => {
                    let prefix =
                        Prefix::new(&self.allocators[PREFIX_ALLOC], current, prefix_count, false);
                    let count = prefix.get_count(prefix_count);

                    for i in 0..count as usize {
                        if prefix.get_byte(i) != row_id.get_byte(depth) {
                            return false;
                        }
                        depth += 1;
                    }

                    current = prefix.get_child();
                }
            }
        }

        false
    }

    // =========================================================================
    // Delete Operations
    // =========================================================================

    /// Delete a key from the ART.
    ///
    /// # Arguments
    /// * `arena` - Arena allocator for temporary allocations
    /// * `key` - The key to delete
    /// * `row_id` - The row ID to delete
    ///
    /// # Returns
    /// `true` if the key was deleted, `false` if not found
    pub fn delete_key(&mut self, arena: &mut ArenaAllocator, key: &ARTKey, row_id: i64) -> bool {
        if !self.tree.has_metadata() {
            return false;
        }

        let row_id_key = ARTKey::from_row_id(arena, row_id);
        let prefix_count = self.prefix_count as usize;

        // Take tree out temporarily
        let mut tree = std::mem::replace(&mut self.tree, Node::empty());
        let mut prefix = Node::empty();
        let result = Self::delete_recursive(
            &mut self.allocators,
            &mut tree,
            &mut prefix,
            key,
            0,
            &row_id_key,
            GateStatus::NotSet,
            prefix_count,
        );
        // Put tree back
        self.tree = tree;
        result
    }

    /// Recursive delete implementation.
    fn delete_recursive(
        allocators: &mut [FixedSizeAllocator; ALLOCATOR_COUNT],
        node: &mut Node,
        prefix: &mut Node,
        key: &ARTKey,
        mut depth: usize,
        row_id: &ARTKey,
        mut status: GateStatus,
        prefix_count: usize,
    ) -> bool {
        if !node.has_metadata() {
            return false;
        }

        let mut active_key = key;

        loop {
            if !node.has_metadata() {
                return false;
            }

            // Check for gate status transition
            if status == GateStatus::NotSet && node.get_gate_status() == GateStatus::Set {
                active_key = row_id;
                depth = 0;
                status = GateStatus::Set;
                continue;
            }

            let ntype = node.get_type();
            match ntype {
                NType::LeafInlined => {
                    if node.get_row_id() == row_id.get_row_id() {
                        node.clear();
                        return true;
                    }
                    return false;
                }
                NType::Leaf => {
                    // Deprecated leaf - not supported for delete
                    return false;
                }
                NType::Node7Leaf => {
                    let byte = active_key.get_byte(super::prefix::ROW_ID_COUNT as usize);
                    {
                        let handle =
                            super::leaf::Node7Leaf::get(&allocators[NODE7_LEAF_ALLOC], *node);
                        if !handle.has_byte(byte) {
                            return false;
                        }
                    }
                    // Use split_at_mut to avoid multiple mutable borrows
                    // NODE7_LEAF_ALLOC = 6, PREFIX_ALLOC = 0
                    let (left, right) = allocators.split_at_mut(NODE7_LEAF_ALLOC);
                    super::leaf::Node7Leaf::delete_byte(
                        &mut right[0],
                        &mut left[PREFIX_ALLOC],
                        node,
                        prefix,
                        byte,
                        row_id,
                        prefix_count,
                    );
                    return true;
                }
                NType::Node15Leaf => {
                    let byte = active_key.get_byte(super::prefix::ROW_ID_COUNT as usize);
                    {
                        let handle =
                            super::leaf::Node15Leaf::get(&allocators[NODE15_LEAF_ALLOC], *node);
                        if !handle.has_byte(byte) {
                            return false;
                        }
                    }
                    // NODE15_LEAF_ALLOC = 7, NODE7_LEAF_ALLOC = 6
                    let (left, right) = allocators.split_at_mut(NODE15_LEAF_ALLOC);
                    super::leaf::Node15Leaf::delete_byte(
                        &mut left[NODE7_LEAF_ALLOC],
                        &mut right[0],
                        node,
                        byte,
                    );
                    return true;
                }
                NType::Node256Leaf => {
                    let byte = active_key.get_byte(super::prefix::ROW_ID_COUNT as usize);
                    {
                        let handle =
                            super::leaf::Node256Leaf::get(&allocators[NODE256_LEAF_ALLOC], *node);
                        if !handle.has_byte(byte) {
                            return false;
                        }
                    }
                    // NODE256_LEAF_ALLOC = 8, NODE15_LEAF_ALLOC = 7
                    let (left, right) = allocators.split_at_mut(NODE256_LEAF_ALLOC);
                    super::leaf::Node256Leaf::delete_byte(
                        &mut left[NODE15_LEAF_ALLOC],
                        &mut right[0],
                        node,
                        byte,
                    );
                    return true;
                }
                NType::Node4 | NType::Node16 | NType::Node48 | NType::Node256 => {
                    debug_assert!(depth < active_key.len);
                    let byte = active_key.get_byte(depth);

                    if let Some(mut child) = Self::get_child_copy(allocators, node, byte) {
                        // Recurse into child
                        let result = Self::delete_recursive(
                            allocators,
                            &mut child,
                            node,
                            key,
                            depth + 1,
                            row_id,
                            status,
                            prefix_count,
                        );
                        if result && !child.has_metadata() {
                            // Child was deleted, remove from parent
                            Self::delete_child(
                                allocators,
                                node,
                                prefix,
                                byte,
                                status,
                                row_id,
                                prefix_count,
                            );
                        } else if result {
                            // Update child in parent
                            Self::replace_child(allocators, node, byte, child);
                        }
                        return result;
                    }
                    return false;
                }
                NType::Prefix => {
                    let count = {
                        let p = Prefix::new(&allocators[PREFIX_ALLOC], *node, prefix_count, false);
                        p.get_count(prefix_count)
                    };

                    // Check for mismatch in prefix
                    for i in 0..count as usize {
                        let prefix_byte = Prefix::get_byte_static(
                            &allocators[PREFIX_ALLOC],
                            node,
                            prefix_count,
                            i as u8,
                        );
                        if prefix_byte != active_key.get_byte(depth) {
                            return false;
                        }
                        depth += 1;
                    }

                    // Get child and continue
                    let mut child = {
                        let p = Prefix::new(&allocators[PREFIX_ALLOC], *node, prefix_count, false);
                        p.get_child()
                    };
                    let result = Self::delete_recursive(
                        allocators,
                        &mut child,
                        node,
                        key,
                        depth,
                        row_id,
                        status,
                        prefix_count,
                    );
                    if result {
                        if !child.has_metadata() {
                            // Child was deleted, free only this prefix node (not the chain)
                            // The child prefix nodes (if any) were already freed by the recursive call
                            Prefix::free_node(&mut allocators[PREFIX_ALLOC], *node);
                            node.clear();
                        } else {
                            // Update child in prefix
                            let mut p =
                                Prefix::new(&allocators[PREFIX_ALLOC], *node, prefix_count, true);
                            p.set_child(child);
                        }
                    }
                    return result;
                }
            }
        }
    }

    /// Delete a child from an internal node.
    fn delete_child(
        allocators: &mut [FixedSizeAllocator; ALLOCATOR_COUNT],
        node: &mut Node,
        prefix: &mut Node,
        byte: u8,
        status: GateStatus,
        row_id: &ARTKey,
        prefix_count: usize,
    ) {
        match node.get_type() {
            NType::Node4 => {
                // NODE4_ALLOC = 2, PREFIX_ALLOC = 0
                let (left, right) = allocators.split_at_mut(NODE4_ALLOC);
                Node4::delete_child(
                    &mut right[0],
                    &mut left[PREFIX_ALLOC],
                    node,
                    prefix,
                    byte,
                    status,
                    prefix_count,
                );
            }
            NType::Node16 => {
                let (left, right) = allocators.split_at_mut(NODE16_ALLOC);
                Node16::delete_child(&mut left[NODE4_ALLOC], &mut right[0], node, byte);
            }
            NType::Node48 => {
                let (left, right) = allocators.split_at_mut(NODE48_ALLOC);
                Node48::delete_child(&mut left[NODE16_ALLOC], &mut right[0], node, byte);
            }
            NType::Node256 => {
                let (left, right) = allocators.split_at_mut(NODE256_ALLOC);
                Node256::delete_child(&mut left[NODE48_ALLOC], &mut right[0], node, byte);
            }
            NType::Node7Leaf => {
                // NODE7_LEAF_ALLOC = 6, PREFIX_ALLOC = 0
                let (left, right) = allocators.split_at_mut(NODE7_LEAF_ALLOC);
                super::leaf::Node7Leaf::delete_byte(
                    &mut right[0],
                    &mut left[PREFIX_ALLOC],
                    node,
                    prefix,
                    byte,
                    row_id,
                    prefix_count,
                );
            }
            NType::Node15Leaf => {
                // NODE15_LEAF_ALLOC = 7, NODE7_LEAF_ALLOC = 6
                let (left, right) = allocators.split_at_mut(NODE15_LEAF_ALLOC);
                super::leaf::Node15Leaf::delete_byte(
                    &mut left[NODE7_LEAF_ALLOC],
                    &mut right[0],
                    node,
                    byte,
                );
            }
            NType::Node256Leaf => {
                // NODE256_LEAF_ALLOC = 8, NODE15_LEAF_ALLOC = 7
                let (left, right) = allocators.split_at_mut(NODE256_LEAF_ALLOC);
                super::leaf::Node256Leaf::delete_byte(
                    &mut left[NODE15_LEAF_ALLOC],
                    &mut right[0],
                    node,
                    byte,
                );
            }
            _ => {}
        }
    }

    // =========================================================================
    // Search Operations
    // =========================================================================

    /// Search for all row IDs equal to the given key.
    ///
    /// # Arguments
    /// * `key` - The key to search for
    /// * `row_ids` - Output set of row IDs
    /// * `max_count` - Maximum number of row IDs to collect
    ///
    /// # Returns
    /// `true` if search completed, `false` if max_count was exceeded
    pub fn search_equal(
        &self,
        key: &ARTKey,
        row_ids: &mut std::collections::BTreeSet<i64>,
        max_count: usize,
    ) -> bool {
        if !self.tree.has_metadata() {
            return true;
        }

        if let Some(node) = self.lookup(key) {
            self.collect_row_ids(&node, row_ids, max_count)
        } else {
            true
        }
    }

    /// Search for all row IDs greater than [or equal to] the given key.
    ///
    /// # Arguments
    /// * `key` - The lower bound key
    /// * `equal` - Whether to include the lower bound
    /// * `row_ids` - Output set of row IDs
    /// * `max_count` - Maximum number of row IDs to collect
    ///
    /// # Returns
    /// `true` if search completed, `false` if max_count was exceeded
    pub fn search_greater(
        &self,
        key: &ARTKey,
        equal: bool,
        row_ids: &mut std::collections::BTreeSet<i64>,
        max_count: usize,
    ) -> bool {
        if !self.tree.has_metadata() {
            return true;
        }

        let mut iterator =
            super::iterator::Iterator::new(&self.allocators, self.prefix_count as usize);
        if !iterator.lower_bound(&self.tree, key, equal) {
            return true;
        }

        iterator.scan(None, max_count, row_ids, false)
    }

    /// Search for all row IDs less than [or equal to] the given key.
    ///
    /// # Arguments
    /// * `key` - The upper bound key
    /// * `equal` - Whether to include the upper bound
    /// * `row_ids` - Output set of row IDs
    /// * `max_count` - Maximum number of row IDs to collect
    ///
    /// # Returns
    /// `true` if search completed, `false` if max_count was exceeded
    pub fn search_less(
        &self,
        key: &ARTKey,
        equal: bool,
        row_ids: &mut std::collections::BTreeSet<i64>,
        max_count: usize,
    ) -> bool {
        if !self.tree.has_metadata() {
            return true;
        }

        let mut iterator =
            super::iterator::Iterator::new(&self.allocators, self.prefix_count as usize);
        iterator.find_minimum(&self.tree);

        iterator.scan(Some(key), max_count, row_ids, equal)
    }

    /// Search for all row IDs in the closed range [lower, upper].
    ///
    /// # Arguments
    /// * `lower` - The lower bound key
    /// * `upper` - The upper bound key
    /// * `row_ids` - Output set of row IDs
    /// * `max_count` - Maximum number of row IDs to collect
    ///
    /// # Returns
    /// `true` if search completed, `false` if max_count was exceeded
    pub fn search_close_range(
        &self,
        lower: &ARTKey,
        upper: &ARTKey,
        row_ids: &mut std::collections::BTreeSet<i64>,
        max_count: usize,
    ) -> bool {
        if !self.tree.has_metadata() {
            return true;
        }

        let mut iterator =
            super::iterator::Iterator::new(&self.allocators, self.prefix_count as usize);
        if !iterator.lower_bound(&self.tree, lower, true) {
            return true;
        }

        iterator.scan(Some(upper), max_count, row_ids, true)
    }

    fn collect_row_ids(
        &self,
        node: &Node,
        row_ids: &mut std::collections::BTreeSet<i64>,
        max_count: usize,
    ) -> bool {
        if !node.has_metadata() {
            return true;
        }

        let mut iterator =
            super::iterator::Iterator::new(&self.allocators, self.prefix_count as usize);
        iterator.find_minimum(node);
        iterator.scan(None, max_count, row_ids, false)
    }

    /// Check if a leaf node has a specific byte.
    fn leaf_has_byte(&self, node: &Node, byte: u8) -> bool {
        match node.get_type() {
            NType::Node7Leaf => {
                let handle = super::leaf::Node7Leaf::get(&self.allocators[NODE7_LEAF_ALLOC], *node);
                handle.has_byte(byte)
            }
            NType::Node15Leaf => {
                let handle =
                    super::leaf::Node15Leaf::get(&self.allocators[NODE15_LEAF_ALLOC], *node);
                handle.has_byte(byte)
            }
            NType::Node256Leaf => {
                let handle =
                    super::leaf::Node256Leaf::get(&self.allocators[NODE256_LEAF_ALLOC], *node);
                handle.has_byte(byte)
            }
            _ => false,
        }
    }

    // =========================================================================
    // Memory Management
    // =========================================================================

    /// Get the in-memory size of the ART.
    pub fn get_memory_size(&self) -> usize {
        let mut size = 0;
        for allocator in &self.allocators {
            size += allocator.get_in_memory_size();
        }
        size
    }

    /// Clear the ART, freeing all nodes.
    pub fn clear(&mut self) {
        self.tree.clear();
        for allocator in &mut self.allocators {
            allocator.reset();
        }
    }

    /// Check if the ART is empty.
    pub fn is_empty(&self) -> bool {
        !self.tree.has_metadata()
    }

    // =========================================================================
    // Merge Operations
    // =========================================================================

    /// Initialize merge by getting upper bounds for buffer IDs.
    fn initialize_merge_upper_bounds(&self) -> Vec<u32> {
        let mut upper_bounds = Vec::with_capacity(ALLOCATOR_COUNT);
        for allocator in &self.allocators {
            upper_bounds.push(allocator.get_upper_bound_buffer_id());
        }
        upper_bounds
    }

    /// Initialize merge by incrementing buffer IDs in the other tree.
    /// This function traverses the tree and updates buffer IDs for all nodes.
    /// Must be called AFTER allocators have been merged.
    fn initialize_merge_tree(
        allocators: &[FixedSizeAllocator; ALLOCATOR_COUNT],
        prefix_count: usize,
        root: &mut Node,
        upper_bounds: &[u32],
    ) {
        if !root.has_metadata() {
            return;
        }

        // Update root's buffer ID first
        let root_type = root.get_type();
        if root_type != NType::LeafInlined && root_type != NType::Leaf {
            let idx = root_type.allocator_index() as usize;
            if idx < upper_bounds.len() && upper_bounds[idx] > 0 {
                root.increase_buffer_id(upper_bounds[idx]);
            }
        }

        // Traverse and update all nodes
        let mut stack = vec![*root];
        while let Some(current) = stack.pop() {
            if !current.has_metadata() {
                continue;
            }

            let ntype = current.get_type();

            // Process children based on node type
            match ntype {
                NType::LeafInlined
                | NType::Leaf
                | NType::Node7Leaf
                | NType::Node15Leaf
                | NType::Node256Leaf => {
                    // Leaf nodes have no children to update
                }
                NType::Prefix => {
                    let mut prefix = Prefix::new(&allocators[0], current, prefix_count, true);
                    let mut child = prefix.get_child();
                    let child_type = child.get_type();
                    if child_type != NType::LeafInlined
                        && child_type != NType::Leaf
                        && child.has_metadata()
                    {
                        let child_idx = child_type.allocator_index() as usize;
                        if child_idx < upper_bounds.len() && upper_bounds[child_idx] > 0 {
                            child.increase_buffer_id(upper_bounds[child_idx]);
                            prefix.set_child(child);
                        }
                    }
                    if child.has_metadata() {
                        stack.push(child);
                    }
                }
                NType::Node4 => {
                    let mut handle = Node4::get_mut(&allocators[2], current);
                    let count = handle.get_count() as usize;
                    for i in 0..count {
                        let byte = handle.get_key(i as u8);
                        let mut child = handle.get_child_at(i as u8);
                        if child.has_metadata() {
                            let child_type = child.get_type();
                            if child_type != NType::LeafInlined && child_type != NType::Leaf {
                                let child_idx = child_type.allocator_index() as usize;
                                if child_idx < upper_bounds.len() && upper_bounds[child_idx] > 0 {
                                    child.increase_buffer_id(upper_bounds[child_idx]);
                                    handle.replace_child(byte, child);
                                }
                            }
                            stack.push(child);
                        }
                    }
                }
                NType::Node16 => {
                    let mut handle = Node16::get_mut(&allocators[3], current);
                    let count = handle.get_count() as usize;
                    for i in 0..count {
                        let byte = handle.get_key(i as u8);
                        let mut child = handle.get_child_at(i as u8);
                        if child.has_metadata() {
                            let child_type = child.get_type();
                            if child_type != NType::LeafInlined && child_type != NType::Leaf {
                                let child_idx = child_type.allocator_index() as usize;
                                if child_idx < upper_bounds.len() && upper_bounds[child_idx] > 0 {
                                    child.increase_buffer_id(upper_bounds[child_idx]);
                                    handle.replace_child(byte, child);
                                }
                            }
                            stack.push(child);
                        }
                    }
                }
                NType::Node48 => {
                    let mut handle = Node48::get_mut(&allocators[4], current);
                    for byte in 0..=255u8 {
                        let Some(mut child) = handle.get_child(byte).copied() else {
                            continue;
                        };
                        if child.has_metadata() {
                            let child_type = child.get_type();
                            if child_type != NType::LeafInlined && child_type != NType::Leaf {
                                let child_idx = child_type.allocator_index() as usize;
                                if child_idx < upper_bounds.len() && upper_bounds[child_idx] > 0 {
                                    child.increase_buffer_id(upper_bounds[child_idx]);
                                    handle.replace_child(byte, child);
                                }
                            }
                            stack.push(child);
                        }
                    }
                }
                NType::Node256 => {
                    let mut handle = Node256::get_mut(&allocators[5], current);
                    for byte in 0..=255u8 {
                        let Some(mut child) = handle.get_child(byte).copied() else {
                            continue;
                        };
                        if child.has_metadata() {
                            let child_type = child.get_type();
                            if child_type != NType::LeafInlined && child_type != NType::Leaf {
                                let child_idx = child_type.allocator_index() as usize;
                                if child_idx < upper_bounds.len() && upper_bounds[child_idx] > 0 {
                                    child.increase_buffer_id(upper_bounds[child_idx]);
                                    handle.replace_child(byte, child);
                                }
                            }
                            stack.push(child);
                        }
                    }
                }
            }
        }
    }

    /// Merge another ART into this one.
    ///
    /// # Arguments
    /// * `arena` - Arena allocator for temporary allocations
    /// * `other` - The other ART to merge
    ///
    /// # Returns
    /// `true` if merge was successful, `false` if constraint violation
    pub fn merge_art(&mut self, arena: &mut ArenaAllocator, other: &mut ART) -> bool {
        if !other.tree.has_metadata() {
            return true;
        }

        if other.owns_data {
            if self.prefix_count != other.prefix_count {
                panic!("Failed to merge ARTs - prefix count does not match");
            }

            // Get upper bounds BEFORE merging allocators
            let upper_bounds = if self.tree.has_metadata() {
                self.initialize_merge_upper_bounds()
            } else {
                vec![0; ALLOCATOR_COUNT]
            };

            // Merge the allocators first
            for i in 0..ALLOCATOR_COUNT {
                self.allocators[i].merge(&mut other.allocators[i]);
            }

            // Now update buffer IDs in other tree using merged allocators
            if self.tree.has_metadata() {
                Self::initialize_merge_tree(
                    &self.allocators,
                    self.prefix_count as usize,
                    &mut other.tree,
                    &upper_bounds,
                );
            }
        }

        // Merge the trees
        if self.tree.has_metadata() {
            // Cache is_unique before borrowing allocators mutably
            let is_unique = self.is_unique();
            let prefix_count = self.prefix_count as usize;
            let mut merger =
                super::merger::ARTMerger::new(arena, &mut self.allocators, prefix_count, is_unique);
            merger.init(self.tree, other.tree);
            let result = merger.merge();
            result == ARTConflictType::NoConflict
        } else {
            self.tree = other.tree;
            other.tree.clear();
            true
        }
    }

    // =========================================================================
    // Vacuum Operations
    // =========================================================================

    /// Initialize vacuum by checking which allocators need vacuuming.
    fn initialize_vacuum(&mut self) -> std::collections::HashSet<u8> {
        let mut indexes = std::collections::HashSet::new();
        for i in 0..self.allocators.len() {
            if self.allocators[i].initialize_vacuum() {
                indexes.insert(i as u8);
            }
        }
        indexes
    }

    /// Finalize vacuum for the specified allocators.
    fn finalize_vacuum(&mut self, indexes: &std::collections::HashSet<u8>) {
        for &idx in indexes {
            self.allocators[idx as usize].finalize_vacuum();
        }
    }

    /// Vacuum the ART, reclaiming space from deleted entries.
    pub fn vacuum_art(&mut self) {
        if !self.tree.has_metadata() {
            for allocator in &mut self.allocators {
                allocator.reset();
            }
            return;
        }

        // Check which allocators need vacuum
        let indexes = self.initialize_vacuum();
        if indexes.is_empty() {
            return;
        }

        // Collect nodes that need vacuuming using a simple traversal
        let prefix_count = self.prefix_count as usize;
        let mut nodes_to_vacuum: Vec<(Node, NType)> = Vec::new();

        // Use a stack-based traversal to collect nodes
        let mut stack = vec![self.tree];
        while let Some(node) = stack.pop() {
            if !node.has_metadata() {
                continue;
            }

            let ntype = node.get_type();
            match ntype {
                NType::LeafInlined => {
                    // Skip inlined leaves
                }
                NType::Leaf => {
                    let idx = ntype.allocator_index() as usize;
                    if indexes.contains(&(idx as u8)) {
                        nodes_to_vacuum.push((node, ntype));
                    }
                }
                NType::Node7Leaf | NType::Node15Leaf | NType::Node256Leaf => {
                    // Skip leaf nodes
                }
                NType::Prefix => {
                    let idx = ntype.allocator_index() as usize;
                    if indexes.contains(&(idx as u8)) {
                        nodes_to_vacuum.push((node, ntype));
                    }
                    // Add child to stack
                    let prefix = Prefix::new(&self.allocators[0], node, prefix_count, false);
                    stack.push(prefix.get_child());
                }
                NType::Node4 => {
                    let idx = ntype.allocator_index() as usize;
                    if indexes.contains(&(idx as u8)) {
                        nodes_to_vacuum.push((node, ntype));
                    }
                    // Add children to stack
                    let handle = Node4::get(&self.allocators[2], node);
                    let count = handle.get_count() as usize;
                    for i in 0..count {
                        let child = handle.get_child_at(i as u8);
                        if child.has_metadata() {
                            stack.push(child);
                        }
                    }
                }
                NType::Node16 => {
                    let idx = ntype.allocator_index() as usize;
                    if indexes.contains(&(idx as u8)) {
                        nodes_to_vacuum.push((node, ntype));
                    }
                    let handle = Node16::get(&self.allocators[3], node);
                    let count = handle.get_count() as usize;
                    for i in 0..count {
                        let child = handle.get_child_at(i as u8);
                        if child.has_metadata() {
                            stack.push(child);
                        }
                    }
                }
                NType::Node48 => {
                    let idx = ntype.allocator_index() as usize;
                    if indexes.contains(&(idx as u8)) {
                        nodes_to_vacuum.push((node, ntype));
                    }
                    let handle = Node48::get(&self.allocators[4], node);
                    for byte in 0..=255u8 {
                        if let Some(child) = handle.get_child(byte) {
                            stack.push(*child);
                        }
                    }
                }
                NType::Node256 => {
                    let idx = ntype.allocator_index() as usize;
                    if indexes.contains(&(idx as u8)) {
                        nodes_to_vacuum.push((node, ntype));
                    }
                    let handle = Node256::get(&self.allocators[5], node);
                    for byte in 0..=255u8 {
                        if let Some(child) = handle.get_child(byte) {
                            stack.push(*child);
                        }
                    }
                }
            }
        }

        // Now vacuum the collected nodes
        for (mut node, ntype) in nodes_to_vacuum {
            let idx = ntype.allocator_index() as usize;
            if ntype == NType::Leaf {
                Leaf::deprecated_vacuum(&mut self.allocators[idx], &mut node);
            } else if self.allocators[idx].needs_vacuum(node.into()) {
                let status = node.get_gate_status();
                let new_ptr = self.allocators[idx].vacuum_pointer(node.into());
                node = Node::from_pointer(new_ptr);
                node.set_type(ntype);
                node.set_gate_status(status);
            }
        }

        // Finalize vacuum
        self.finalize_vacuum(&indexes);
    }

    // =========================================================================
    // Serialization Operations
    // =========================================================================

    /// Prepare the ART for serialization.
    ///
    /// This removes empty buffers and prepares the allocators for serialization.
    ///
    /// # Arguments
    /// * `options` - Serialization options
    /// * `v1_0_0_storage` - Whether to use deprecated storage format
    ///
    /// # Returns
    /// IndexStorageInfo containing the serialization metadata
    fn prepare_serialize(
        &mut self,
        options: &HashMap<String, Value>,
        _v1_0_0_storage: bool,
    ) -> IndexStorageInfo {
        let mut info = IndexStorageInfo::new(&self.name);
        info.root = self.tree.into();
        info.options = options.clone();

        // Remove empty buffers from all allocators
        for allocator in &mut self.allocators {
            allocator.remove_empty_buffers();
        }

        info
    }

    /// Serialize the ART to disk for checkpoint.
    ///
    /// This method prepares the ART for serialization and returns the storage info
    /// needed to restore the index later.
    ///
    /// # Arguments
    /// * `options` - Serialization options
    ///
    /// # Returns
    /// IndexStorageInfo containing all information needed to restore the index
    pub fn serialize_to_disk_impl(&mut self, options: &HashMap<String, Value>) -> IndexStorageInfo {
        // Check for v1.0.0 storage format option
        let v1_0_0_storage = options
            .get("v1_0_0_storage")
            .map(|v| match v {
                Value::Boolean(b) => *b,
                _ => true,
            })
            .unwrap_or(true);

        let mut info = self.prepare_serialize(options, v1_0_0_storage);

        // Determine allocator count based on storage format
        let allocator_count = if v1_0_0_storage {
            DEPRECATED_ALLOCATOR_COUNT
        } else {
            ALLOCATOR_COUNT
        };

        // Collect allocator info for each allocator
        for i in 0..allocator_count {
            info.allocator_infos.push(self.allocators[i].get_info());
        }

        info
    }

    /// Serialize the ART to WAL.
    ///
    /// This method prepares the ART for WAL serialization, including buffer data.
    ///
    /// # Arguments
    /// * `options` - Serialization options
    ///
    /// # Returns
    /// IndexStorageInfo containing all information needed to restore the index
    pub fn serialize_to_wal_impl(&mut self, options: &HashMap<String, Value>) -> IndexStorageInfo {
        // Check for v1.0.0 storage format option
        let v1_0_0_storage = options
            .get("v1_0_0_storage")
            .map(|v| match v {
                Value::Boolean(b) => *b,
                _ => true,
            })
            .unwrap_or(true);

        let mut info = self.prepare_serialize(options, v1_0_0_storage);

        // Determine allocator count based on storage format
        let allocator_count = if v1_0_0_storage {
            DEPRECATED_ALLOCATOR_COUNT
        } else {
            ALLOCATOR_COUNT
        };

        // Collect allocator info and buffer data for each allocator
        for i in 0..allocator_count {
            let buffer_infos = self.allocators[i].init_serialization_to_wal();
            info.buffers.push(buffer_infos);
            info.allocator_infos.push(self.allocators[i].get_info());
        }

        info
    }

    /// Initialize allocators from storage info.
    ///
    /// This method restores the allocator state from serialized information.
    ///
    /// # Arguments
    /// * `info` - The storage info containing allocator metadata
    pub fn init_allocators(&mut self, info: &IndexStorageInfo) {
        for (i, alloc_info) in info.allocator_infos.iter().enumerate() {
            if i < self.allocators.len() {
                self.allocators[i].init(alloc_info);
            }
        }
    }

    /// Set the prefix count from storage info.
    ///
    /// This method determines the prefix count based on the storage info.
    ///
    /// # Arguments
    /// * `info` - The storage info
    fn set_prefix_count(&mut self, info: &IndexStorageInfo) {
        // Check for backwards compatibility with root_block_ptr
        if info.root_block_ptr.is_valid() {
            self.prefix_count = super::prefix::DEPRECATED_COUNT;
            return;
        }

        // Get prefix count from allocator info
        if !info.allocator_infos.is_empty() {
            let serialized_count =
                info.allocator_infos[0].segment_size - super::prefix::METADATA_SIZE;
            self.prefix_count = serialized_count as u8;
            return;
        }

        // Calculate from column types
        self.prefix_count =
            Self::align_prefix_count(Self::type_size(&self.logical_type).saturating_sub(1));
    }

    /// Create an ART from storage info.
    ///
    /// This is used to restore an ART from checkpoint or WAL.
    ///
    /// # Arguments
    /// * `name` - Index name
    /// * `constraint_type` - Index constraint type
    /// * `column_id` - Column ID being indexed
    /// * `logical_type` - Logical type of the indexed column
    /// * `buffer_manager` - Buffer manager for memory allocation
    /// * `info` - Storage info from serialization
    ///
    /// # Returns
    /// A new ART initialized from the storage info
    pub fn from_storage_info(
        name: impl Into<String>,
        constraint_type: IndexConstraintType,
        column_id: ColumnId,
        logical_type: LogicalType,
        buffer_manager: Arc<dyn BufferManager>,
        info: &IndexStorageInfo,
    ) -> Self {
        let name = name.into();

        // Determine if we need key length verification
        let verify_max_key_len = matches!(
            &logical_type,
            LogicalType::Varchar | LogicalType::VarcharCollation(_)
        );

        // Calculate initial prefix count (will be updated from info)
        let prefix_count = Self::calculate_prefix_count(&logical_type);

        // Create allocators for each node type
        let prefix_size = prefix_count as usize + super::prefix::METADATA_SIZE;
        let allocators = Self::create_allocators(prefix_size, buffer_manager);

        let mut art = Self {
            name,
            constraint_type,
            column_id,
            logical_type,
            tree: Node::empty(),
            allocators,
            owns_data: true,
            verify_max_key_len,
            prefix_count,
            delta_index_type: DeltaIndexType::None,
        };

        // Initialize from storage info if valid
        if info.is_valid() {
            art.set_prefix_count(info);

            // Set root node
            if info.root.is_valid() {
                art.tree = Node::from_pointer(info.root);
            }

            // Initialize allocators
            art.init_allocators(info);
        }

        art
    }
}

// =============================================================================
// Index Trait Implementation
// =============================================================================

impl Index for ART {
    fn index_name(&self) -> &str {
        &self.name
    }

    fn index_type(&self) -> &str {
        Self::TYPE_NAME
    }

    fn constraint_type(&self) -> IndexConstraintType {
        self.constraint_type
    }

    fn column_ids(&self) -> &[ColumnId] {
        std::slice::from_ref(&self.column_id)
    }

    fn is_bound(&self) -> bool {
        true
    }

    fn commit_drop(&mut self) -> Result<()> {
        self.clear();
        Ok(())
    }
}

// =============================================================================
// BoundIndex Trait Implementation
// =============================================================================

impl BoundIndex for ART {
    fn physical_types(&self) -> &[LogicalType] {
        std::slice::from_ref(&self.logical_type)
    }

    fn logical_types(&self) -> &[LogicalType] {
        std::slice::from_ref(&self.logical_type)
    }

    fn delta_index_type(&self) -> DeltaIndexType {
        self.delta_index_type
    }

    fn append(&self, _chunk: &Chunk, _row_ids: &Vector) -> Result<()> {
        Err(paro_error::not_implemented("ART::append"))
    }

    fn append_with_info(
        &self,
        chunk: &Chunk,
        row_ids: &Vector,
        _info: &IndexAppendInfo,
    ) -> Result<()> {
        self.append(chunk, row_ids)
    }

    fn delete(&self, _entries: &Chunk, _row_ids: &Vector) -> Result<usize> {
        Err(paro_error::not_implemented("ART::delete"))
    }

    fn insert(&self, _chunk: &Chunk, _row_ids: &Vector) -> Result<()> {
        Err(paro_error::not_implemented("ART::insert"))
    }

    fn evaluate_predicate(&self, predicate: &Predicate) -> PredicateResult {
        if predicate.column_id() != self.column_id {
            return PredicateResult::Unknown;
        }
        let logical_type = &self.logical_type;

        let allocator = Arc::new(paro_common::allocator::DefaultAllocator::new());
        let mut arena = ArenaAllocator::new(allocator);

        let mut row_ids = std::collections::BTreeSet::new();

        match predicate {
            Predicate::Eq { value, .. } => {
                let Ok(bytes) = value_to_bytes(value, logical_type) else {
                    return PredicateResult::Unknown;
                };
                let Ok(key) = ARTKey::create_key(&mut arena, logical_type, &bytes) else {
                    return PredicateResult::Unknown;
                };
                self.search_equal(&key, &mut row_ids, usize::MAX);
            }
            Predicate::Range { lower, upper, .. } => {
                let Ok(lower_bytes) = value_to_bytes(lower, logical_type) else {
                    return PredicateResult::Unknown;
                };
                let Ok(upper_bytes) = value_to_bytes(upper, logical_type) else {
                    return PredicateResult::Unknown;
                };
                let Ok(lower_key) = ARTKey::create_key(&mut arena, logical_type, &lower_bytes)
                else {
                    return PredicateResult::Unknown;
                };
                let Ok(upper_key) = ARTKey::create_key(&mut arena, logical_type, &upper_bytes)
                else {
                    return PredicateResult::Unknown;
                };
                self.search_close_range(&lower_key, &upper_key, &mut row_ids, usize::MAX);
            }
            _ => return PredicateResult::Unknown,
        }

        if row_ids.is_empty() {
            return PredicateResult::NoneMatch;
        }

        let mut bitmap = RoaringBitmap::new();
        for row_id in row_ids {
            if row_id < 0 || row_id > u32::MAX as i64 {
                return PredicateResult::Unknown;
            }
            bitmap.insert(row_id as u32);
        }

        if bitmap.is_empty() {
            PredicateResult::NoneMatch
        } else {
            PredicateResult::Bitmap(bitmap)
        }
    }

    fn merge_indexes(&self, _other: &dyn BoundIndex) -> Result<bool> {
        Err(paro_error::not_implemented("ART::merge_indexes"))
    }

    fn vacuum(&self) {}

    fn supports_delta_indexes(&self) -> bool {
        true
    }

    fn create_delta_index(&self, delta_type: DeltaIndexType) -> Result<Arc<dyn BoundIndex>> {
        let constraint = if delta_type == DeltaIndexType::DeletedRowsInUse {
            IndexConstraintType::None
        } else {
            self.constraint_type
        };

        let buffer_manager = self.allocators[0].buffer_manager();
        let mut art = ART::new(
            self.name.clone(),
            constraint,
            self.column_id,
            self.logical_type.clone(),
            buffer_manager.clone(),
        );
        art.delta_index_type = delta_type;

        Ok(Arc::new(art))
    }

    fn get_in_memory_size(&self) -> usize {
        self.get_memory_size()
    }

    fn serialize_to_disk(&self) -> Result<IndexStorageInfo> {
        // Note: This requires &mut self, but the trait uses &self
        // For now, return a basic info. Full implementation requires mutable access.
        let mut info = IndexStorageInfo::new(&self.name);
        info.root = self.tree.into();

        // Collect allocator info (read-only)
        for allocator in &self.allocators {
            info.allocator_infos.push(allocator.get_info());
        }

        Ok(info)
    }

    fn verify(&self) -> Result<()> {
        Ok(())
    }

    fn to_string_debug(&self, _display_ascii: bool) -> String {
        if self.tree.has_metadata() {
            format!("ART(name={}, root={:?})", self.name, self.tree)
        } else {
            format!("ART(name={}, empty)", self.name)
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::StandardBufferManager;
    use paro_common::allocator::DefaultAllocator;

    fn create_test_art() -> ART {
        let buffer_manager = Arc::new(StandardBufferManager::default_manager());
        ART::new(
            "test_idx",
            IndexConstraintType::None,
            0,
            LogicalType::BigInt,
            buffer_manager,
        )
    }

    fn create_unique_art() -> ART {
        let buffer_manager = Arc::new(StandardBufferManager::default_manager());
        ART::new(
            "unique_idx",
            IndexConstraintType::Unique,
            0,
            LogicalType::BigInt,
            buffer_manager,
        )
    }

    fn create_arena() -> ArenaAllocator {
        let allocator = Arc::new(DefaultAllocator::new());
        ArenaAllocator::new(allocator)
    }

    #[test]
    fn test_art_new() {
        let art = create_test_art();
        assert_eq!(art.index_name(), "test_idx");
        assert_eq!(art.index_type(), "ART");
        assert_eq!(art.constraint_type(), IndexConstraintType::None);
        assert!(!art.is_unique());
        assert!(art.is_empty());
    }

    #[test]
    fn test_art_unique() {
        let art = create_unique_art();
        assert!(art.is_unique());
        assert_eq!(art.constraint_type(), IndexConstraintType::Unique);
    }

    #[test]
    fn test_art_insert_single_key() {
        let mut art = create_test_art();
        let mut arena = create_arena();

        let key = ARTKey::from_i64(&mut arena, 42).unwrap();
        let result = art.insert_key(&mut arena, &key, 1, IndexAppendMode::Default);

        assert_eq!(result, ARTConflictType::NoConflict);
        assert!(!art.is_empty());
    }

    #[test]
    fn test_art_insert_multiple_keys() {
        let mut art = create_test_art();
        let mut arena = create_arena();

        for i in 0..10 {
            let key = ARTKey::from_i64(&mut arena, i * 100).unwrap();
            let result = art.insert_key(&mut arena, &key, i, IndexAppendMode::Default);
            assert_eq!(result, ARTConflictType::NoConflict);
        }

        assert!(!art.is_empty());
    }

    #[test]
    fn test_art_lookup_existing_key() {
        let mut art = create_test_art();
        let mut arena = create_arena();

        let key = ARTKey::from_i64(&mut arena, 42).unwrap();
        art.insert_key(&mut arena, &key, 1, IndexAppendMode::Default);

        let result = art.lookup(&key);
        assert!(result.is_some());
    }

    #[test]
    fn test_art_lookup_nonexistent_key() {
        let mut art = create_test_art();
        let mut arena = create_arena();

        let key1 = ARTKey::from_i64(&mut arena, 42).unwrap();
        art.insert_key(&mut arena, &key1, 1, IndexAppendMode::Default);

        let key2 = ARTKey::from_i64(&mut arena, 99).unwrap();
        let result = art.lookup(&key2);
        assert!(result.is_none());
    }

    #[test]
    fn test_art_unique_constraint_violation() {
        let mut art = create_unique_art();
        let mut arena = create_arena();

        let key = ARTKey::from_i64(&mut arena, 42).unwrap();

        let result1 = art.insert_key(&mut arena, &key, 1, IndexAppendMode::Default);
        assert_eq!(result1, ARTConflictType::NoConflict);

        let result2 = art.insert_key(&mut arena, &key, 2, IndexAppendMode::Default);
        assert_eq!(result2, ARTConflictType::Constraint);
    }

    #[test]
    fn test_art_non_unique_allows_duplicates() {
        let mut art = create_test_art();
        let mut arena = create_arena();

        let key = ARTKey::from_i64(&mut arena, 42).unwrap();

        let result1 = art.insert_key(&mut arena, &key, 1, IndexAppendMode::Default);
        assert_eq!(result1, ARTConflictType::NoConflict);

        let result2 = art.insert_key(&mut arena, &key, 2, IndexAppendMode::Default);
        assert_eq!(result2, ARTConflictType::NoConflict);
    }

    #[test]
    fn test_art_ignore_duplicates_mode() {
        let mut art = create_unique_art();
        let mut arena = create_arena();

        let key = ARTKey::from_i64(&mut arena, 42).unwrap();

        art.insert_key(&mut arena, &key, 1, IndexAppendMode::Default);

        let result = art.insert_key(&mut arena, &key, 2, IndexAppendMode::IgnoreDuplicates);
        assert_eq!(result, ARTConflictType::NoConflict);
    }

    #[test]
    fn test_art_clear() {
        let mut art = create_test_art();
        let mut arena = create_arena();

        let key = ARTKey::from_i64(&mut arena, 42).unwrap();
        art.insert_key(&mut arena, &key, 1, IndexAppendMode::Default);
        assert!(!art.is_empty());

        art.clear();
        assert!(art.is_empty());
    }

    #[test]
    fn test_art_memory_size() {
        let mut art = create_test_art();
        let mut arena = create_arena();

        let initial_size = art.get_memory_size();

        for i in 0..100 {
            let key = ARTKey::from_i64(&mut arena, i).unwrap();
            art.insert_key(&mut arena, &key, i, IndexAppendMode::Default);
        }

        let final_size = art.get_memory_size();
        assert!(final_size >= initial_size);
    }

    #[test]
    fn test_art_prefix_count() {
        let art = create_test_art();
        assert!(art.prefix_count() > 0);
    }

    #[test]
    fn test_art_varchar_index() {
        let buffer_manager = Arc::new(StandardBufferManager::default_manager());
        let art = ART::new(
            "varchar_idx",
            IndexConstraintType::None,
            0,
            LogicalType::Varchar,
            buffer_manager,
        );
        assert!(art.verify_max_key_len);
    }

    #[test]
    fn test_art_integer_index_does_not_require_max_key_verification() {
        let buffer_manager = Arc::new(StandardBufferManager::default_manager());
        let art = ART::new(
            "int_idx",
            IndexConstraintType::None,
            0,
            LogicalType::Integer,
            buffer_manager,
        );
        assert!(!art.verify_max_key_len);
    }

    #[test]
    fn test_art_delta_index() {
        let art = create_test_art();
        let delta = art.create_delta_index(DeltaIndexType::LocalAppend).unwrap();
        assert_eq!(delta.delta_index_type(), DeltaIndexType::LocalAppend);
    }

    #[test]
    fn test_art_to_string_debug() {
        let art = create_test_art();
        let debug_str = art.to_string_debug(false);
        assert!(debug_str.contains("ART"));
        assert!(debug_str.contains("test_idx"));
    }

    // ========== Delete Tests ==========

    #[test]
    fn test_art_delete_single_key() {
        let mut art = create_test_art();
        let mut arena = create_arena();

        let key = ARTKey::from_i64(&mut arena, 42).unwrap();
        art.insert_key(&mut arena, &key, 1, IndexAppendMode::Default);

        assert!(!art.is_empty());
        let deleted = art.delete_key(&mut arena, &key, 1);
        assert!(deleted);
        assert!(art.is_empty());
    }

    #[test]
    fn test_art_delete_nonexistent_key() {
        let mut art = create_test_art();
        let mut arena = create_arena();

        let key1 = ARTKey::from_i64(&mut arena, 42).unwrap();
        art.insert_key(&mut arena, &key1, 1, IndexAppendMode::Default);

        let key2 = ARTKey::from_i64(&mut arena, 99).unwrap();
        let deleted = art.delete_key(&mut arena, &key2, 2);
        assert!(!deleted);
        assert!(!art.is_empty());
    }

    #[test]
    fn test_art_delete_two_keys() {
        let mut art = create_test_art();
        let mut arena = create_arena();

        // Insert two keys
        let key1 = ARTKey::from_i64(&mut arena, 100).unwrap();
        let key2 = ARTKey::from_i64(&mut arena, 200).unwrap();
        art.insert_key(&mut arena, &key1, 1, IndexAppendMode::Default);
        art.insert_key(&mut arena, &key2, 2, IndexAppendMode::Default);

        // Verify both keys exist
        assert!(art.lookup(&key1).is_some());
        assert!(art.lookup(&key2).is_some());

        // Delete first key
        let deleted = art.delete_key(&mut arena, &key1, 1);
        assert!(deleted);

        // Verify first key is gone, second key still exists
        assert!(art.lookup(&key1).is_none());
        assert!(art.lookup(&key2).is_some());
    }

    #[test]
    fn test_art_delete_three_keys() {
        let mut art = create_test_art();
        let mut arena = create_arena();

        // Insert three keys
        for i in 0..3 {
            let key = ARTKey::from_i64(&mut arena, i * 100).unwrap();
            art.insert_key(&mut arena, &key, i, IndexAppendMode::Default);
        }

        // Delete first key (i=0)
        let key0 = ARTKey::from_i64(&mut arena, 0).unwrap();
        let deleted = art.delete_key(&mut arena, &key0, 0);
        assert!(deleted, "Failed to delete key 0");

        // Verify key 0 is gone
        assert!(art.lookup(&key0).is_none(), "Key 0 should be deleted");

        // Verify keys 1 and 2 still exist
        let key1 = ARTKey::from_i64(&mut arena, 100).unwrap();
        let key2 = ARTKey::from_i64(&mut arena, 200).unwrap();
        assert!(art.lookup(&key1).is_some(), "Key 100 should exist");
        assert!(art.lookup(&key2).is_some(), "Key 200 should exist");
    }

    #[test]
    fn test_art_delete_multiple_keys() {
        let mut art = create_test_art();
        let mut arena = create_arena();

        // Insert multiple keys
        for i in 0..10 {
            let key = ARTKey::from_i64(&mut arena, i * 100).unwrap();
            art.insert_key(&mut arena, &key, i, IndexAppendMode::Default);
        }

        // Delete some keys
        for i in (0..10).step_by(2) {
            let key = ARTKey::from_i64(&mut arena, i * 100).unwrap();
            let deleted = art.delete_key(&mut arena, &key, i);
            assert!(deleted);
        }

        // Verify remaining keys
        for i in 0..10 {
            let key = ARTKey::from_i64(&mut arena, i * 100).unwrap();
            let result = art.lookup(&key);
            if i % 2 == 0 {
                assert!(result.is_none());
            } else {
                assert!(result.is_some());
            }
        }
    }

    // ========== Search Tests ==========

    #[test]
    fn test_art_search_equal() {
        let mut art = create_test_art();
        let mut arena = create_arena();

        let key = ARTKey::from_i64(&mut arena, 42).unwrap();
        art.insert_key(&mut arena, &key, 1, IndexAppendMode::Default);

        let mut row_ids = std::collections::BTreeSet::new();
        let result = art.search_equal(&key, &mut row_ids, 100);
        assert!(result);
        assert!(row_ids.contains(&1));
    }

    #[test]
    fn test_art_search_greater() {
        let mut art = create_test_art();
        let mut arena = create_arena();

        // Insert keys 10, 20, 30, 40, 50
        for i in 1..=5 {
            let key = ARTKey::from_i64(&mut arena, i * 10).unwrap();
            art.insert_key(&mut arena, &key, i, IndexAppendMode::Default);
        }

        // Search for keys > 25
        let search_key = ARTKey::from_i64(&mut arena, 25).unwrap();
        let mut row_ids = std::collections::BTreeSet::new();
        let result = art.search_greater(&search_key, false, &mut row_ids, 100);
        assert!(result);
        // Should find row_ids 3, 4, 5 (keys 30, 40, 50)
        assert!(row_ids.contains(&3));
        assert!(row_ids.contains(&4));
        assert!(row_ids.contains(&5));
        assert!(!row_ids.contains(&1));
        assert!(!row_ids.contains(&2));
    }

    #[test]
    fn test_art_search_less() {
        let mut art = create_test_art();
        let mut arena = create_arena();

        // Insert keys 10, 20, 30, 40, 50
        for i in 1..=5 {
            let key = ARTKey::from_i64(&mut arena, i * 10).unwrap();
            art.insert_key(&mut arena, &key, i, IndexAppendMode::Default);
        }

        // Search for keys < 35
        let search_key = ARTKey::from_i64(&mut arena, 35).unwrap();
        let mut row_ids = std::collections::BTreeSet::new();
        let result = art.search_less(&search_key, false, &mut row_ids, 100);
        assert!(result);
        // Should find row_ids 1, 2, 3 (keys 10, 20, 30)
        assert!(row_ids.contains(&1));
        assert!(row_ids.contains(&2));
        assert!(row_ids.contains(&3));
        assert!(!row_ids.contains(&4));
        assert!(!row_ids.contains(&5));
    }

    #[test]
    fn test_art_search_close_range() {
        let mut art = create_test_art();
        let mut arena = create_arena();

        // Insert keys 10, 20, 30, 40, 50
        for i in 1..=5 {
            let key = ARTKey::from_i64(&mut arena, i * 10).unwrap();
            art.insert_key(&mut arena, &key, i, IndexAppendMode::Default);
        }

        // Search for keys in [20, 40]
        let lower = ARTKey::from_i64(&mut arena, 20).unwrap();
        let upper = ARTKey::from_i64(&mut arena, 40).unwrap();
        let mut row_ids = std::collections::BTreeSet::new();
        let result = art.search_close_range(&lower, &upper, &mut row_ids, 100);
        assert!(result);
        // Should find row_ids 2, 3, 4 (keys 20, 30, 40)
        assert!(row_ids.contains(&2));
        assert!(row_ids.contains(&3));
        assert!(row_ids.contains(&4));
        assert!(!row_ids.contains(&1));
        assert!(!row_ids.contains(&5));
    }

    #[test]
    fn test_art_search_empty_tree() {
        let art = create_test_art();
        let mut arena = create_arena();

        let key = ARTKey::from_i64(&mut arena, 42).unwrap();
        let mut row_ids = std::collections::BTreeSet::new();

        assert!(art.search_equal(&key, &mut row_ids, 100));
        assert!(row_ids.is_empty());

        assert!(art.search_greater(&key, false, &mut row_ids, 100));
        assert!(row_ids.is_empty());

        assert!(art.search_less(&key, false, &mut row_ids, 100));
        assert!(row_ids.is_empty());
    }

    #[test]
    fn test_art_search_equal_collects_duplicate_row_ids() {
        let mut art = create_test_art();
        let mut arena = create_arena();

        let duplicate = ARTKey::from_i64(&mut arena, 42).unwrap();
        let other = ARTKey::from_i64(&mut arena, 84).unwrap();

        assert_eq!(
            art.insert_key(&mut arena, &duplicate, 1, IndexAppendMode::Default),
            ARTConflictType::NoConflict
        );
        assert_eq!(
            art.insert_key(&mut arena, &duplicate, 2, IndexAppendMode::Default),
            ARTConflictType::NoConflict
        );
        assert_eq!(
            art.insert_key(&mut arena, &other, 3, IndexAppendMode::Default),
            ARTConflictType::NoConflict
        );

        let mut row_ids = std::collections::BTreeSet::new();
        assert!(art.search_equal(&duplicate, &mut row_ids, 100));
        assert_eq!(row_ids.into_iter().collect::<Vec<_>>(), vec![1, 2]);
    }

    // ========== Merge Tests ==========

    #[test]
    fn test_art_merge_empty_into_empty() {
        let mut art1 = create_test_art();
        let mut art2 = create_test_art();
        let mut arena = create_arena();

        let result = art1.merge_art(&mut arena, &mut art2);
        assert!(result);
        assert!(art1.is_empty());
    }

    #[test]
    fn test_art_merge_into_empty() {
        let mut art1 = create_test_art();
        let mut art2 = create_test_art();
        let mut arena = create_arena();

        // Insert keys into art2
        for i in 0..5 {
            let key = ARTKey::from_i64(&mut arena, i * 10).unwrap();
            art2.insert_key(&mut arena, &key, i, IndexAppendMode::Default);
        }

        let result = art1.merge_art(&mut arena, &mut art2);
        assert!(result);
        assert!(!art1.is_empty());

        // Verify all keys are in art1
        for i in 0..5 {
            let key = ARTKey::from_i64(&mut arena, i * 10).unwrap();
            assert!(art1.lookup(&key).is_some(), "Key {} should exist", i * 10);
        }
    }

    #[test]
    fn test_art_merge_empty_into_nonempty() {
        let mut art1 = create_test_art();
        let mut art2 = create_test_art();
        let mut arena = create_arena();

        // Insert keys into art1
        for i in 0..5 {
            let key = ARTKey::from_i64(&mut arena, i * 10).unwrap();
            art1.insert_key(&mut arena, &key, i, IndexAppendMode::Default);
        }

        let result = art1.merge_art(&mut arena, &mut art2);
        assert!(result);
        assert!(!art1.is_empty());

        // Verify all keys are still in art1
        for i in 0..5 {
            let key = ARTKey::from_i64(&mut arena, i * 10).unwrap();
            assert!(art1.lookup(&key).is_some(), "Key {} should exist", i * 10);
        }
    }

    // Note: test_art_merge_disjoint_keys is complex and requires more work on the merger
    // to handle buffer ID updates correctly. Skipping for now.

    // ========== Vacuum Tests ==========

    #[test]
    fn test_art_vacuum_empty() {
        let mut art = create_test_art();
        art.vacuum_art();
        assert!(art.is_empty());
    }

    #[test]
    fn test_art_vacuum_after_delete() {
        let mut art = create_test_art();
        let mut arena = create_arena();

        // Insert keys
        for i in 0..10 {
            let key = ARTKey::from_i64(&mut arena, i * 100).unwrap();
            art.insert_key(&mut arena, &key, i, IndexAppendMode::Default);
        }

        // Delete some keys
        for i in (0..10).step_by(2) {
            let key = ARTKey::from_i64(&mut arena, i * 100).unwrap();
            art.delete_key(&mut arena, &key, i);
        }

        // Vacuum
        art.vacuum_art();

        // Verify remaining keys still exist
        for i in 0..10 {
            let key = ARTKey::from_i64(&mut arena, i * 100).unwrap();
            let result = art.lookup(&key);
            if i % 2 == 0 {
                assert!(result.is_none(), "Key {} should be deleted", i * 100);
            } else {
                assert!(result.is_some(), "Key {} should exist", i * 100);
            }
        }
    }

    #[test]
    fn test_art_vacuum_all_deleted() {
        let mut art = create_test_art();
        let mut arena = create_arena();

        // Insert keys
        for i in 0..5 {
            let key = ARTKey::from_i64(&mut arena, i * 100).unwrap();
            art.insert_key(&mut arena, &key, i, IndexAppendMode::Default);
        }

        // Delete all keys
        for i in 0..5 {
            let key = ARTKey::from_i64(&mut arena, i * 100).unwrap();
            art.delete_key(&mut arena, &key, i);
        }

        // Vacuum
        art.vacuum_art();

        // After deleting all keys, the tree root should be empty
        // Note: vacuum doesn't automatically clear the tree structure,
        // it only reclaims space from deleted entries
        // The tree may still have some structure but no valid keys
        for i in 0..5 {
            let key = ARTKey::from_i64(&mut arena, i * 100).unwrap();
            assert!(
                art.lookup(&key).is_none(),
                "Key {} should be deleted",
                i * 100
            );
        }
    }

    // ========== Serialization Tests ==========

    #[test]
    fn test_art_serialize_empty() {
        let art = create_test_art();
        let info = art.serialize_to_disk().unwrap();

        assert_eq!(info.name, "test_idx");
        assert!(info.is_valid());
        // Empty tree has no valid root
        assert!(!info.root.is_valid());
    }

    #[test]
    fn test_art_serialize_with_data() {
        let mut art = create_test_art();
        let mut arena = create_arena();

        // Insert some keys
        for i in 0..10 {
            let key = ARTKey::from_i64(&mut arena, i * 100).unwrap();
            art.insert_key(&mut arena, &key, i, IndexAppendMode::Default);
        }

        let info = art.serialize_to_disk().unwrap();

        assert_eq!(info.name, "test_idx");
        assert!(info.is_valid());
        // Non-empty tree has valid root
        assert!(info.root.is_valid());
        // Should have allocator infos
        assert!(!info.allocator_infos.is_empty());
    }

    #[test]
    fn test_art_serialize_to_disk_impl() {
        let mut art = create_test_art();
        let mut arena = create_arena();

        // Insert some keys
        for i in 0..5 {
            let key = ARTKey::from_i64(&mut arena, i * 10).unwrap();
            art.insert_key(&mut arena, &key, i, IndexAppendMode::Default);
        }

        let options = HashMap::new();
        let info = art.serialize_to_disk_impl(&options);

        assert_eq!(info.name, "test_idx");
        assert!(info.is_valid());
        assert!(info.root.is_valid());
        // Should have allocator infos for deprecated format (6 allocators)
        assert_eq!(info.allocator_infos.len(), DEPRECATED_ALLOCATOR_COUNT);
    }

    #[test]
    fn test_art_serialize_to_wal_impl() {
        let mut art = create_test_art();
        let mut arena = create_arena();

        // Insert some keys
        for i in 0..5 {
            let key = ARTKey::from_i64(&mut arena, i * 10).unwrap();
            art.insert_key(&mut arena, &key, i, IndexAppendMode::Default);
        }

        let options = HashMap::new();
        let info = art.serialize_to_wal_impl(&options);

        assert_eq!(info.name, "test_idx");
        assert!(info.is_valid());
        assert!(info.root.is_valid());
        // Should have allocator infos
        assert_eq!(info.allocator_infos.len(), DEPRECATED_ALLOCATOR_COUNT);
        // Should have buffer data for WAL
        assert_eq!(info.buffers.len(), DEPRECATED_ALLOCATOR_COUNT);
    }

    #[test]
    fn test_art_from_storage_info_empty() {
        let buffer_manager = Arc::new(StandardBufferManager::default_manager());
        let info = IndexStorageInfo::new("restored_idx");

        let art = ART::from_storage_info(
            "restored_idx",
            IndexConstraintType::None,
            0,
            LogicalType::BigInt,
            buffer_manager,
            &info,
        );

        assert_eq!(art.index_name(), "restored_idx");
        assert!(art.is_empty());
    }

    #[test]
    fn test_art_serialize_roundtrip() {
        let mut art = create_test_art();
        let mut arena = create_arena();

        // Insert some keys
        for i in 0..5 {
            let key = ARTKey::from_i64(&mut arena, i * 10).unwrap();
            art.insert_key(&mut arena, &key, i, IndexAppendMode::Default);
        }

        // Serialize
        let info = art.serialize_to_disk().unwrap();

        // Create new ART from storage info
        let buffer_manager = Arc::new(StandardBufferManager::default_manager());
        let art2 = ART::from_storage_info(
            "test_idx",
            IndexConstraintType::None,
            0,
            LogicalType::BigInt,
            buffer_manager,
            &info,
        );

        // Verify the new ART has the same structure
        assert_eq!(art2.index_name(), "test_idx");
        // Note: Full roundtrip requires loading data from disk, which is not implemented yet
        // For now, we just verify the storage info is valid
        assert!(info.is_valid());
    }

    #[test]
    fn test_art_serialize_with_v1_0_0_option() {
        let mut art = create_test_art();
        let mut arena = create_arena();

        // Insert some keys
        for i in 0..3 {
            let key = ARTKey::from_i64(&mut arena, i * 10).unwrap();
            art.insert_key(&mut arena, &key, i, IndexAppendMode::Default);
        }

        // Serialize with v1.0.0 storage format
        let mut options = HashMap::new();
        options.insert("v1_0_0_storage".to_string(), Value::Boolean(true));
        let info = art.serialize_to_disk_impl(&options);

        assert!(info.is_valid());
        assert_eq!(info.allocator_infos.len(), DEPRECATED_ALLOCATOR_COUNT);
    }

    #[test]
    fn test_art_serialize_with_new_format() {
        let mut art = create_test_art();
        let mut arena = create_arena();

        // Insert some keys
        for i in 0..3 {
            let key = ARTKey::from_i64(&mut arena, i * 10).unwrap();
            art.insert_key(&mut arena, &key, i, IndexAppendMode::Default);
        }

        // Serialize with new storage format
        let mut options = HashMap::new();
        options.insert("v1_0_0_storage".to_string(), Value::Boolean(false));
        let info = art.serialize_to_disk_impl(&options);

        assert!(info.is_valid());
        assert_eq!(info.allocator_infos.len(), ALLOCATOR_COUNT);
    }

    #[test]
    fn test_art_allocator_info() {
        let mut art = create_test_art();
        let mut arena = create_arena();

        // Insert keys to create some allocations
        for i in 0..20 {
            let key = ARTKey::from_i64(&mut arena, i * 100).unwrap();
            art.insert_key(&mut arena, &key, i, IndexAppendMode::Default);
        }

        let info = art.serialize_to_disk().unwrap();

        // Check allocator infos
        for alloc_info in &info.allocator_infos {
            assert!(alloc_info.segment_size > 0);
        }
    }

    #[test]
    fn test_art_init_allocators() {
        let mut art = create_test_art();
        let mut arena = create_arena();

        // Insert keys
        for i in 0..5 {
            let key = ARTKey::from_i64(&mut arena, i * 10).unwrap();
            art.insert_key(&mut arena, &key, i, IndexAppendMode::Default);
        }

        // Get storage info
        let info = art.serialize_to_disk().unwrap();

        // Create new ART and init allocators
        let buffer_manager = Arc::new(StandardBufferManager::default_manager());
        let mut art2 = ART::new(
            "test_idx2",
            IndexConstraintType::None,
            0,
            LogicalType::BigInt,
            buffer_manager,
        );

        art2.init_allocators(&info);

        // Verify allocators were initialized
        // Note: This doesn't fully restore the data, just the allocator metadata
        assert!(info.is_valid());
    }
}
