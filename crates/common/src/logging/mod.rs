//! # Logging System
//!
//! Provides logging initialization and utilities for Paro.
//!
//! ## Quick Start
//!
//! ### Simple initialization (console only)
//!
//! ```rust,ignore
//! use paro_common::logging;
//!
//! // Initialize with config
//! logging::init(&config.logging);
//!
//! // Or use default (console, INFO level)
//! logging::init_default();
//!
//! // Now use tracing macros anywhere
//! tracing::info!("Server started");
//! ```
//!
//! ### Advanced initialization (with LogManager)
//!
//! ```rust,ignore
//! use paro_common::logging::LogManager;
//! use paro_common::config::LoggingConfig;
//!
//! // Initialize with runtime control
//! let manager = LogManager::init(LoggingConfig::default())?;
//!
//! // Change log level at runtime
//! manager.set_level(LogLevel::Debug)?;
//!
//! // Query stored logs
//! let entries = manager.memory_storage().all();
//! ```
//!
//! ## Usage (no logger instance needed!)
//!
//! ```rust,ignore
//! use tracing::{info, debug, warn, error};
//!
//! fn my_function() {
//!     info!("Simple message");
//!     info!(user_id = 123, "With structured fields");
//!     debug!("Debug info: {}", some_value);
//!     error!(error = %e, "Error occurred");
//! }
//! ```
//!
//! ## Convenience macros
//!
//! ```rust,ignore
//! use paro_common::{paro_info, paro_debug, paro_error};
//!
//! paro_info!("Uses module path as target");
//! ```

mod init;
mod layer;
mod macros;
mod manager;
mod storage;
pub mod targets;

pub use init::{init, init_default, init_from_env};
pub use manager::{LogError, LogManager};
pub use storage::{LogEntry, LogQueryFilter, MemoryLogStorage, SpanInfo};
