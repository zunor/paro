//! # ART (Adaptive Radix Tree) runtime predicate index
//!
//! ART now serves as a segment-local, single-column scalar predicate index.
//! `CREATE INDEX` only stages metadata; runtime hooks build one in-memory ART
//! per visible segment during post-commit attach, compaction rebuild, and WAL
//! recovery.
//!
//! ## Architecture
//!
//! The runtime ART stack consists of:
//! - `ARTKey`: radix-encoded scalar keys
//! - `Node` / `NType`: adaptive node headers and variants
//! - `Prefix`: prefix-compressed path segments
//! - `Leaf`: row-id payload nodes, including duplicate-key fan-out subtrees
//! - `InternalNode`: Node4/16/48/256 branching nodes
//! - `ART`: the main in-memory index implementing `BoundIndex`
//! - `ARTScanner`: tree traversal for diagnostics / maintenance
//! - `ARTMerger`: merge support for ART trees

mod art;
mod art_key;
pub mod compaction;
mod internal_node;
mod iterator;
mod leaf;
mod merger;
mod node;
mod prefix;
mod scanner;

pub use art::{ARTConflictType, ART};
pub use art_key::{ARTKey, Radix, MAX_KEY_LEN};
pub use internal_node::{Node16, Node256, Node4, Node48};
pub use iterator::{Iterator as ARTIterator, IteratorEntry, IteratorKey};
pub use leaf::{
    DeprecatedLeaf, Leaf, Node15Leaf, Node256Leaf, Node7Leaf, AND_LAST_BYTE, DEPRECATED_LEAF_SIZE,
    MAX_ROW_ID_LOCAL,
};
pub use merger::ARTMerger;
pub use node::{GateStatus, NType, Node, ALLOCATOR_COUNT, DEPRECATED_ALLOCATOR_COUNT};
pub use prefix::{Prefix, DEPRECATED_COUNT, METADATA_SIZE, ROW_ID_COUNT, ROW_ID_SIZE};
pub use scanner::{ARTHandlingResult, ARTScanHandling, ARTScanner};
