// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::database::handle::{AttachVisibility, RecoveryMode};
pub use paro_common::identity::DatabaseType;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

/// Reserved database names that cannot be used for user databases.
pub const RESERVED_NAMES: &[&str] = &["system", "temp", "main"];

/// Immutable identity/configuration for a database attachment.
pub struct DatabaseIdentity {
    /// Instance-wide unique database identifier.
    pub id: u64,
    /// Database name.
    pub name: String,
    /// Physical path to the database data (directory or file).
    pub path: String,
    /// Type of this database.
    pub db_type: DatabaseType,
    /// Recovery mode for this database.
    pub recovery_mode: RecoveryMode,
    /// Visibility of this database.
    pub visibility: AttachVisibility,
    /// Whether this is the initial (main) database.
    pub is_initial_database: AtomicBool,
    /// Additional attach options.
    pub attach_options: HashMap<String, String>,
}

impl DatabaseIdentity {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: u64,
        name: String,
        path: String,
        db_type: DatabaseType,
        recovery_mode: RecoveryMode,
        visibility: AttachVisibility,
        is_initial_database: bool,
        attach_options: HashMap<String, String>,
    ) -> Self {
        Self {
            id,
            name,
            path,
            db_type,
            recovery_mode,
            visibility,
            is_initial_database: AtomicBool::new(is_initial_database),
            attach_options,
        }
    }

    pub fn is_initial_database(&self) -> bool {
        self.is_initial_database.load(Ordering::Acquire)
    }

    pub fn set_initial_database(&self) {
        self.is_initial_database.store(true, Ordering::Release);
    }
    pub fn name_is_reserved(name: &str) -> bool {
        RESERVED_NAMES.contains(&name.to_lowercase().as_str())
    }

    pub fn extract_database_name(dbpath: &str) -> String {
        if dbpath.is_empty() || dbpath == ":memory:" {
            return "memory".to_string();
        }

        let path = dbpath.split('?').next().unwrap_or(dbpath);
        let name = std::path::PathBuf::from(path)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("database")
            .to_string();

        if Self::name_is_reserved(&name) {
            format!("{}_db", name)
        } else {
            name
        }
    }
}
