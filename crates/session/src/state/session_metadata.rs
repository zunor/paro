use std::sync::RwLock;

use paro_context::{
    CursorSummary, PreparedStatementSummary, SessionMetadataProvider, SessionMetadataRows,
    SettingRow,
};

#[derive(Debug, Default)]
pub struct SharedSessionMetadataState {
    inner: RwLock<SessionMetadataRows>,
}

impl SharedSessionMetadataState {
    pub fn replace(&self, rows: SessionMetadataRows) {
        if let Ok(mut guard) = self.inner.write() {
            *guard = rows;
        }
    }
}

impl SessionMetadataProvider for SharedSessionMetadataState {
    fn current_settings(&self) -> Vec<SettingRow> {
        self.inner
            .read()
            .map(|guard| guard.settings.clone())
            .unwrap_or_default()
    }

    fn current_prepared_statements(&self) -> Vec<PreparedStatementSummary> {
        self.inner
            .read()
            .map(|guard| guard.prepared_statements.clone())
            .unwrap_or_default()
    }

    fn current_cursors(&self) -> Vec<CursorSummary> {
        self.inner
            .read()
            .map(|guard| guard.cursors.clone())
            .unwrap_or_default()
    }
}
