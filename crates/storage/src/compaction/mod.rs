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
