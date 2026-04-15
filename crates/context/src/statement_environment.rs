use paro_catalog::search_path::CatalogSearchEntry;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StatementAuthContext {
    pub active_role: Option<String>,
    pub tenant: Option<String>,
    pub authenticated_user: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatementEnvironment {
    pub current_database: String,
    pub current_schema: String,
    pub current_user: String,
    pub search_path: Vec<CatalogSearchEntry>,
    pub auth: StatementAuthContext,
}
