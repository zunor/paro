// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Configuration System
//!
//! Provides a unified, layered configuration system for Paro.
//!
//! ## Configuration Priority (low to high)
//!
//! 1. Code defaults (`Default` trait)
//! 2. System config file (`/etc/paro/paro.toml`)
//! 3. User config file (`~/.config/paro/paro.toml`)
//! 4. Current directory (`./paro.toml`)
//! 5. Custom config file (`--config path`)
//! 6. Environment variables (`PARO_*`)
//!
//! ## Example
//!
//! ```rust,ignore
//! use paro_common::config::{ConfigLoader, ParoConfig};
//! use paro_common::logging;
//!
//! // Load configuration from all sources
//! let config = ConfigLoader::load()?;
//!
//! // Initialize logging with config
//! logging::init(&config.logging);
//!
//! println!("Server port: {}", config.server.port);
//! println!("Max memory: {}", config.cluster.max_memory);
//! ```

mod human_bytes;
mod loader;
mod types;
mod validation;

pub use human_bytes::{format_human_bytes, format_setting_value, parse_human_bytes};
pub use loader::{ConfigError, ConfigLoader};
pub use types::*;
