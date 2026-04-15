// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Compaction framework (primary-key aware).
//!
//! Provides planning, execution, and conflict resolution for rowset compaction.

pub mod cleanup;
pub mod execution;
pub mod plan;
pub mod publish;
pub mod scheduling;

pub mod compaction_executor;
pub mod compaction_manager;
pub mod compaction_task;
