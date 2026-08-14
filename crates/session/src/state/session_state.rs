// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Session-local state that survives across statements.

use std::sync::Arc;

use super::file_opener::FileOpener;
use super::random::RandomEngine;
use crate::prepared::store::{PortalEntry, PreparedState, PreparedStatementEntry};
use paro_catalog::search_path::CatalogSearchPath;
use paro_context::CompileEnvironmentKey;
use paro_execution::query_executor::compiled::CompiledStatement;
use paro_parser::ast::Statement;

/// One-entry MRU for the immutable image behind a repeated Simple Query.
///
/// Runtime snapshots and statement inputs never live here. The environment
/// key covers catalog generations and plan-affecting settings, matching the
/// revalidation contract used by prepared statements.
#[derive(Debug)]
struct SimpleQueryPlanCacheEntry {
    statement: Statement,
    statement_format: Option<String>,
    plan: CompiledStatement,
}

/// Session-level state data.
#[derive(Debug)]
pub struct SessionState {
    pub profiling_enabled: bool,
    pub search_path: CatalogSearchPath,
    pub prepared: PreparedState,
    pub user_name: String,
    pub application_name: String,
    pub random_engine: RandomEngine,
    pub file_search_path: String,
    pub file_opener: Option<Arc<dyn FileOpener>>,
    simple_query_plan: Option<SimpleQueryPlanCacheEntry>,
}

impl SessionState {
    pub fn new(current_database: impl Into<String>, user_name: impl Into<String>) -> Self {
        Self {
            profiling_enabled: false,
            search_path: CatalogSearchPath::new(current_database),
            prepared: PreparedState::default(),
            user_name: user_name.into(),
            application_name: String::new(),
            random_engine: RandomEngine::new(),
            file_search_path: String::new(),
            file_opener: None,
            simple_query_plan: None,
        }
    }

    #[inline]
    pub fn current_user(&self) -> &str {
        &self.user_name
    }

    #[inline]
    pub fn current_schema(&self) -> &str {
        self.search_path.get_default_schema()
    }

    #[inline]
    pub fn current_database(&self) -> &str {
        self.search_path.current_database()
    }

    #[inline]
    pub fn search_path(&self) -> &CatalogSearchPath {
        &self.search_path
    }

    #[inline]
    pub fn search_path_mut(&mut self) -> &mut CatalogSearchPath {
        &mut self.search_path
    }

    pub fn set_current_database(&mut self, database: impl Into<String>) {
        self.search_path.set_current_database(database);
    }

    pub fn enable_profiler(&mut self) {
        self.profiling_enabled = true;
    }

    pub fn disable_profiler(&mut self) {
        self.profiling_enabled = false;
    }

    pub fn is_profiling_enabled(&self) -> bool {
        self.profiling_enabled
    }

    pub fn add_prepared_statement(&mut self, entry: PreparedStatementEntry) {
        self.prepared.insert_statement(entry);
    }

    pub fn has_prepared_statement(&self, name: &str) -> bool {
        self.prepared.has_statement(name)
    }

    pub fn get_prepared_statement(&self, name: &str) -> Option<&PreparedStatementEntry> {
        self.prepared.statement(name)
    }

    pub fn get_prepared_statement_mut(
        &mut self,
        name: &str,
    ) -> Option<&mut PreparedStatementEntry> {
        self.prepared.statement_mut(name)
    }

    pub fn remove_prepared_statement(&mut self, name: &str) -> Option<PreparedStatementEntry> {
        self.prepared.remove_statement(name)
    }

    pub fn clear_prepared_statements(&mut self) {
        self.prepared.clear_statements();
    }

    pub fn set_unnamed_prepared_statement(
        &mut self,
        entry: PreparedStatementEntry,
    ) -> Option<PreparedStatementEntry> {
        self.prepared.set_unnamed_statement(entry)
    }

    pub fn unnamed_prepared_statement(&self) -> Option<&PreparedStatementEntry> {
        self.prepared.unnamed_statement()
    }

    pub fn unnamed_prepared_statement_mut(&mut self) -> Option<&mut PreparedStatementEntry> {
        self.prepared.unnamed_statement_mut()
    }

    pub fn remove_unnamed_prepared_statement(&mut self) -> Option<PreparedStatementEntry> {
        self.prepared.remove_unnamed_statement()
    }

    pub fn add_portal(&mut self, entry: PortalEntry) {
        self.prepared.put_portal(entry);
    }

    pub fn has_portal(&self, name: &str) -> bool {
        self.prepared.has_portal(name)
    }

    pub fn get_portal(&self, name: &str) -> Option<&PortalEntry> {
        self.prepared.portal(name)
    }

    pub fn get_portal_mut(&mut self, name: &str) -> Option<&mut PortalEntry> {
        self.prepared.portal_mut(name)
    }

    pub fn remove_portal(&mut self, name: &str) -> Option<PortalEntry> {
        self.prepared.remove_portal(name)
    }

    pub fn clear_portals(&mut self) {
        self.prepared.clear_portals();
    }

    pub fn set_unnamed_portal(&mut self, entry: PortalEntry) -> Option<PortalEntry> {
        self.prepared.set_unnamed_portal(entry)
    }

    pub fn unnamed_portal(&self) -> Option<&PortalEntry> {
        self.prepared.unnamed_portal()
    }

    pub fn unnamed_portal_mut(&mut self) -> Option<&mut PortalEntry> {
        self.prepared.unnamed_portal_mut()
    }

    pub fn remove_unnamed_portal(&mut self) -> Option<PortalEntry> {
        self.prepared.remove_unnamed_portal()
    }

    pub fn clear_protocol_unnamed_objects(&mut self) -> bool {
        self.prepared.clear_protocol_unnamed_objects()
    }

    pub(crate) fn reusable_simple_query_plan(
        &self,
        statement: &Statement,
        statement_format: Option<&str>,
        environment: &CompileEnvironmentKey,
    ) -> Option<CompiledStatement> {
        let cached = self.simple_query_plan.as_ref()?;
        (cached.statement == *statement
            && cached.statement_format.as_deref() == statement_format
            && cached.plan.compile_environment() == environment)
            .then(|| cached.plan.clone())
    }

    pub(crate) fn publish_simple_query_plan(
        &mut self,
        statement: Statement,
        statement_format: Option<String>,
        plan: CompiledStatement,
    ) {
        self.simple_query_plan = Some(SimpleQueryPlanCacheEntry {
            statement,
            statement_format,
            plan,
        });
    }

    pub fn reset(&mut self, current_database: &str) {
        self.clear_prepared_statements();
        self.clear_portals();
        self.search_path = CatalogSearchPath::new(current_database);
        self.disable_profiler();
        self.application_name.clear();
        self.simple_query_plan = None;
    }

    #[inline]
    pub fn random_engine(&self) -> &RandomEngine {
        &self.random_engine
    }

    #[inline]
    pub fn random(&self) -> f64 {
        self.random_engine.next_random()
    }

    #[inline]
    pub fn set_seed(&self, seed: f64) {
        self.random_engine.set_seed(seed);
    }

    #[inline]
    pub fn file_search_path(&self) -> &str {
        &self.file_search_path
    }

    pub fn set_file_search_path(&mut self, path: impl Into<String>) {
        self.file_search_path = path.into();
    }

    #[inline]
    pub fn file_opener(&self) -> Option<&Arc<dyn FileOpener>> {
        self.file_opener.as_ref()
    }

    pub fn set_file_opener(&mut self, opener: Arc<dyn FileOpener>) {
        self.file_opener = Some(opener);
    }

    pub fn clear_file_opener(&mut self) {
        self.file_opener = None;
    }
}

impl Default for SessionState {
    fn default() -> Self {
        Self::new("paro", "paro")
    }
}

#[cfg(test)]
mod tests {
    use super::super::file_opener::DefaultFileOpener;
    use super::*;
    use crate::prepared::portal::{
        CursorHoldability, FormatCode, PortalExecutionState, ScrollMode,
    };
    use crate::prepared::store::PreparedStatementSource;
    use paro_parser::ast::Statement;

    fn parse_single(sql: &str) -> Statement {
        paro_parser::parse(sql)
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
            .stmt
    }

    fn make_prepared_entry(name: &str, sql: &str) -> PreparedStatementEntry {
        PreparedStatementEntry {
            name: name.to_string(),
            source_sql: sql.to_string(),
            raw_stmt: parse_single(sql),
            parameter_types: Vec::new(),
            result_schema: Vec::new(),
            generic_plan: None,
            generic_plan_uses: 0,
            source: PreparedStatementSource::Sql,
        }
    }

    fn make_portal(name: &str) -> PortalEntry {
        PortalEntry {
            name: name.to_string(),
            statement_ref: crate::prepared::store::PortalStatementRef::None,
            source_sql: "SELECT 1".to_string(),
            raw_stmt: parse_single("SELECT 1"),
            holdability: CursorHoldability::WithoutHold,
            scroll_mode: ScrollMode::NoScroll,
            result_formats: vec![FormatCode::Text],
            result_schema: Vec::new(),
            kind: crate::prepared::store::PortalKind::ClientCopy {
                stmt: Box::new(parse_single("SELECT 1")),
                parameter_env: crate::prepared::typed_parameters::TypedParameterEnv::default(),
            },
            execution_state: PortalExecutionState::Ready,
            snapshot_retention: None,
            completion: None,
            created_generation: 0,
            transaction_owned: true,
        }
    }

    #[test]
    fn test_session_state_new() {
        let state = SessionState::new("testdb", "testuser");
        assert_eq!(state.current_database(), "testdb");
        assert_eq!(state.current_user(), "testuser");
        assert_eq!(state.current_schema(), "public");
        assert_eq!(state.application_name, "");
    }

    #[test]
    fn test_session_state_default() {
        let state = SessionState::default();
        assert_eq!(state.current_database(), "paro");
        assert_eq!(state.current_user(), "paro");
    }

    #[test]
    fn test_session_state_set_database() {
        let mut state = SessionState::new("db1", "user");
        state.set_current_database("db2");
        assert_eq!(state.current_database(), "db2");
    }

    #[test]
    fn test_session_state_reset() {
        let mut state = SessionState::new("db1", "user");
        state.enable_profiler();
        state.add_prepared_statement(make_prepared_entry("stmt1", "SELECT 1"));
        state.add_portal(make_portal("c1"));
        state.application_name = "psql".to_string();

        state.reset("db2");

        assert!(state.prepared.statements().next().is_none());
        assert!(state.prepared.portals().next().is_none());
        assert!(!state.is_profiling_enabled());
        assert_eq!(state.current_database(), "db2");
        assert_eq!(state.application_name, "");
    }

    #[test]
    fn test_session_state_profiler() {
        let mut state = SessionState::default();
        assert!(!state.is_profiling_enabled());

        state.enable_profiler();
        assert!(state.is_profiling_enabled());

        state.disable_profiler();
        assert!(!state.is_profiling_enabled());
    }

    #[test]
    fn test_session_state_prepared_statements() {
        let mut state = SessionState::default();
        state.add_prepared_statement(make_prepared_entry("stmt1", "SELECT 1"));

        assert!(state.get_prepared_statement("stmt1").is_some());
        assert!(state.get_prepared_statement("nonexistent").is_none());

        let removed = state.remove_prepared_statement("stmt1");
        assert!(removed.is_some());
        assert!(state.get_prepared_statement("stmt1").is_none());
    }

    #[test]
    fn test_session_state_portals() {
        let mut state = SessionState::default();
        state.add_portal(make_portal("c1"));

        assert!(state.get_portal("c1").is_some());
        assert!(state.get_portal("missing").is_none());

        let removed = state.remove_portal("c1");
        assert!(removed.is_some());
        assert!(state.get_portal("c1").is_none());
    }

    #[test]
    fn test_session_state_random() {
        let state = SessionState::default();
        for _ in 0..10 {
            let v = state.random();
            assert!((0.0..1.0).contains(&v));
        }
    }

    #[test]
    fn test_session_state_set_seed() {
        let state1 = SessionState::default();
        let state2 = SessionState::default();

        state1.set_seed(0.5);
        state2.set_seed(0.5);

        assert_eq!(state1.random(), state2.random());
    }

    #[test]
    fn test_session_state_file_search_path() {
        let mut state = SessionState::default();
        assert_eq!(state.file_search_path(), "");

        state.set_file_search_path("/tmp");
        assert_eq!(state.file_search_path(), "/tmp");
    }

    #[test]
    fn test_session_state_file_opener() {
        let mut state = SessionState::default();
        assert!(state.file_opener().is_none());

        let opener = Arc::new(DefaultFileOpener);
        state.set_file_opener(opener.clone());
        assert!(state.file_opener().is_some());

        state.clear_file_opener();
        assert!(state.file_opener().is_none());
    }
}
