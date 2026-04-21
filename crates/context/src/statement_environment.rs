// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_catalog::search_path::CatalogSearchEntry;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StatementAuthContext {
    pub active_role: Option<String>,
    pub tenant: Option<String>,
    pub authenticated_user: Option<String>,
    pub can_create_routine: bool,
    pub can_create_elevated_routine: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatementEnvironment {
    pub current_database: String,
    pub current_schema: String,
    pub current_user: String,
    pub search_path: Vec<CatalogSearchEntry>,
    pub auth: StatementAuthContext,
}
