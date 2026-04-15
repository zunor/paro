use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SettingRow {
    pub name: String,
    pub setting: String,
    pub unit: Option<String>,
    pub category: String,
    pub short_desc: Option<String>,
    pub source: String,
    pub vartype: String,
    pub context: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PreparedStatementSummary {
    pub name: String,
    pub statement: String,
    pub parameter_types: String,
    pub from_sql: bool,
    pub generic_plans: i64,
    pub custom_plans: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CursorSummary {
    pub name: String,
    pub statement: String,
    pub is_holdable: bool,
    pub is_binary: bool,
    pub is_scrollable: bool,
}

#[derive(Debug, Clone, Default)]
pub struct SessionMetadataRows {
    pub settings: Vec<SettingRow>,
    pub prepared_statements: Vec<PreparedStatementSummary>,
    pub cursors: Vec<CursorSummary>,
}

pub trait SessionMetadataProvider: Send + Sync {
    fn current_settings(&self) -> Vec<SettingRow>;
    fn current_prepared_statements(&self) -> Vec<PreparedStatementSummary>;
    fn current_cursors(&self) -> Vec<CursorSummary>;
}

impl SessionMetadataProvider for Arc<dyn SessionMetadataProvider> {
    fn current_settings(&self) -> Vec<SettingRow> {
        self.as_ref().current_settings()
    }

    fn current_prepared_statements(&self) -> Vec<PreparedStatementSummary> {
        self.as_ref().current_prepared_statements()
    }

    fn current_cursors(&self) -> Vec<CursorSummary> {
        self.as_ref().current_cursors()
    }
}
