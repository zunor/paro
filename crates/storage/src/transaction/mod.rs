// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Paro Transaction Manager
//!
//!
//!
//! This crate handles transaction management, MVCC version control, and undo buffers.

pub mod bulk_load;
pub mod cleanup_state;
pub mod commit_state;
pub mod descriptor_cleanup;
pub mod lifecycle_action;
pub mod manager;
pub mod overlay_reader;
pub mod participant;
pub mod rollback_state;
pub mod spill;
pub mod txn;
pub mod undo_buffer;
pub mod version_info;
pub mod write_buffer;
