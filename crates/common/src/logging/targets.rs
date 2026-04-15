// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Predefined logging targets for Paro.
//!
//! Use these targets to categorize and filter logs:
//!
//! ```rust,ignore
//! use tracing::info;
//! use paro_common::logging::targets;
//!
//! info!(target: targets::QUERY, query_id = %id, "Query started");
//! info!(target: targets::STORAGE, "Checkpoint completed");
//! ```
//!
//! Filter specific targets via environment variable:
//! ```bash
//! RUST_LOG="paro::query=debug,paro::storage=trace"
//! ```

/// Query execution logs
pub const QUERY: &str = "paro::query";

/// SQL parser logs
pub const PARSER: &str = "paro::parser";

/// Query planner logs
pub const PLANNER: &str = "paro::planner";

/// Query optimizer logs
pub const OPTIMIZER: &str = "paro::optimizer";

/// Query executor logs
pub const EXECUTOR: &str = "paro::executor";

/// Storage engine logs
pub const STORAGE: &str = "paro::storage";

/// Write-ahead log (WAL) logs
pub const WAL: &str = "paro::wal";

/// Transaction logs
pub const TRANSACTION: &str = "paro::transaction";

/// Catalog/metadata logs
pub const CATALOG: &str = "paro::catalog";

/// Client connection logs
pub const CONNECTION: &str = "paro::connection";

/// Session management logs
pub const SESSION: &str = "paro::session";

/// Task scheduler logs
pub const SCHEDULER: &str = "paro::scheduler";

/// Server operation logs
pub const SERVER: &str = "paro::server";

/// Instance management logs
pub const INSTANCE: &str = "paro::instance";

/// Checkpoint logs
pub const CHECKPOINT: &str = "paro::checkpoint";

/// Pipeline execution logs
pub const PIPELINE: &str = "paro::pipeline";

/// Buffer pool logs
pub const BUFFER: &str = "paro::buffer";

/// Function execution logs
pub const FUNCTION: &str = "paro::function";
