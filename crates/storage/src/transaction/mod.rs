//! Paro Transaction Manager
//!
//!
//!
//! This crate handles transaction management, MVCC version control, and undo buffers.

pub mod cleanup_state;
pub mod commit_state;
pub mod descriptor_cleanup;
pub mod manager;
pub mod rollback_state;
pub mod txn;
pub mod undo_buffer;
pub mod version_info;
