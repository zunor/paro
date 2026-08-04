// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use crate::completion::StatementCompletion;
use crate::dispatch::UtilityCommand;
use crate::prepared::typed_parameters::TypedParameterEnv;
use paro_common::types::LogicalType;
use paro_execution::query_executor::compiled::{
    CompiledStatement, ExecutionRequest, ResultColumnDesc,
};
use paro_parser::ast::Statement;

use super::portal::{
    values_to_text, CursorHoldability, FormatCode, PortalExecutionState, PortalSnapshotRetention,
    ScrollMode,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreparedStatementSource {
    Sql,
    Protocol,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PortalStatementRef {
    None,
    Named(String),
    Unnamed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PortalStoreMark {
    pub generation: u64,
}

#[derive(Debug, Clone)]
pub struct PreparedStatementEntry {
    pub name: String,
    pub source_sql: String,
    pub raw_stmt: Statement,
    pub parameter_types: Vec<Option<LogicalType>>,
    pub result_schema: Vec<ResultColumnDesc>,
    pub generic_plan: Option<CompiledStatement>,
    /// Successful generic-plan selections by SQL EXECUTE or protocol Bind.
    pub generic_plan_uses: i64,
    pub source: PreparedStatementSource,
}

#[derive(Debug, Clone)]
pub enum PortalKind {
    Query(ExecutionRequest),
    Materialized,
    Utility(Box<UtilityCommand>),
    ClientCopy {
        stmt: Box<Statement>,
        parameter_env: TypedParameterEnv,
    },
}

#[derive(Debug, Clone)]
pub struct PortalEntry {
    pub name: String,
    pub statement_ref: PortalStatementRef,
    pub source_sql: String,
    pub raw_stmt: Statement,
    pub holdability: CursorHoldability,
    pub scroll_mode: ScrollMode,
    pub result_formats: Vec<FormatCode>,
    pub result_schema: Vec<ResultColumnDesc>,
    pub kind: PortalKind,
    pub execution_state: PortalExecutionState,
    pub snapshot_retention: Option<PortalSnapshotRetention>,
    pub completion: Option<StatementCompletion>,
    pub created_generation: u64,
    pub transaction_owned: bool,
}

#[derive(Debug, Default)]
pub struct PreparedState {
    named_statements: HashMap<String, PreparedStatementEntry>,
    named_portals: HashMap<String, PortalEntry>,
    unnamed_statement: Option<PreparedStatementEntry>,
    unnamed_portal: Option<PortalEntry>,
    portal_generation: u64,
}

impl PreparedState {
    pub fn has_statement(&self, name: &str) -> bool {
        self.named_statements.contains_key(name)
    }

    pub fn insert_statement(&mut self, entry: PreparedStatementEntry) {
        self.named_statements.insert(entry.name.clone(), entry);
    }

    pub fn statement(&self, name: &str) -> Option<&PreparedStatementEntry> {
        self.named_statements.get(name)
    }

    pub fn statement_mut(&mut self, name: &str) -> Option<&mut PreparedStatementEntry> {
        self.named_statements.get_mut(name)
    }

    pub fn remove_statement(&mut self, name: &str) -> Option<PreparedStatementEntry> {
        let removed = self.named_statements.remove(name);
        if removed.is_some() {
            self.remove_portals_for_statement_ref(&PortalStatementRef::Named(name.to_string()));
        }
        removed
    }

    pub fn clear_statements(&mut self) {
        self.named_statements.clear();
        self.unnamed_statement = None;
        self.named_portals
            .retain(|_, portal| matches!(portal.statement_ref, PortalStatementRef::None));
        if self
            .unnamed_portal
            .as_ref()
            .is_some_and(|portal| !matches!(portal.statement_ref, PortalStatementRef::None))
        {
            self.unnamed_portal = None;
        }
    }

    pub fn set_unnamed_statement(
        &mut self,
        mut entry: PreparedStatementEntry,
    ) -> Option<PreparedStatementEntry> {
        entry.name.clear();
        self.remove_portals_for_statement_ref(&PortalStatementRef::Unnamed);
        self.unnamed_statement.replace(entry)
    }

    pub fn unnamed_statement(&self) -> Option<&PreparedStatementEntry> {
        self.unnamed_statement.as_ref()
    }

    pub fn unnamed_statement_mut(&mut self) -> Option<&mut PreparedStatementEntry> {
        self.unnamed_statement.as_mut()
    }

    pub fn remove_unnamed_statement(&mut self) -> Option<PreparedStatementEntry> {
        let removed = self.unnamed_statement.take();
        if removed.is_some() {
            self.remove_portals_for_statement_ref(&PortalStatementRef::Unnamed);
        }
        removed
    }

    pub fn statements(&self) -> impl Iterator<Item = &PreparedStatementEntry> {
        self.named_statements.values()
    }

    pub fn has_portal(&self, name: &str) -> bool {
        self.named_portals.contains_key(name)
    }

    pub fn put_portal(&mut self, mut entry: PortalEntry) {
        self.portal_generation = self.portal_generation.wrapping_add(1);
        entry.created_generation = self.portal_generation;
        self.named_portals.insert(entry.name.clone(), entry);
    }

    pub fn portal(&self, name: &str) -> Option<&PortalEntry> {
        self.named_portals.get(name)
    }

    pub fn portal_mut(&mut self, name: &str) -> Option<&mut PortalEntry> {
        self.named_portals.get_mut(name)
    }

    pub fn remove_portal(&mut self, name: &str) -> Option<PortalEntry> {
        self.named_portals.remove(name)
    }

    pub fn clear_portals(&mut self) {
        self.named_portals.clear();
        self.unnamed_portal = None;
    }

    pub fn set_unnamed_portal(&mut self, mut entry: PortalEntry) -> Option<PortalEntry> {
        self.portal_generation = self.portal_generation.wrapping_add(1);
        entry.name.clear();
        entry.created_generation = self.portal_generation;
        self.unnamed_portal.replace(entry)
    }

    pub fn unnamed_portal(&self) -> Option<&PortalEntry> {
        self.unnamed_portal.as_ref()
    }

    pub fn unnamed_portal_mut(&mut self) -> Option<&mut PortalEntry> {
        self.unnamed_portal.as_mut()
    }

    pub fn remove_unnamed_portal(&mut self) -> Option<PortalEntry> {
        self.unnamed_portal.take()
    }

    pub fn portals(&self) -> impl Iterator<Item = &PortalEntry> {
        self.named_portals.values()
    }

    pub fn clear_protocol_unnamed_objects(&mut self) -> bool {
        let mut changed = self.unnamed_portal.take().is_some();
        let named_before = self.named_portals.len();
        self.named_portals
            .retain(|_, portal| !matches!(portal.statement_ref, PortalStatementRef::Unnamed));
        changed |= self.named_portals.len() != named_before;
        if self
            .unnamed_statement
            .as_ref()
            .is_some_and(|stmt| stmt.source == PreparedStatementSource::Protocol)
        {
            self.unnamed_statement = None;
            changed = true;
        }
        changed
    }

    pub fn current_portal_mark(&self) -> PortalStoreMark {
        PortalStoreMark {
            generation: self.portal_generation,
        }
    }

    pub fn on_transaction_commit(&mut self) {
        self.named_portals.retain(|_, portal| {
            if portal.transaction_owned
                && matches!(portal.holdability, CursorHoldability::WithoutHold)
            {
                return false;
            }
            if portal.transaction_owned {
                portal.transaction_owned = false;
            }
            true
        });
        self.unnamed_portal = None;
    }

    pub fn on_transaction_rollback(&mut self) {
        self.named_portals
            .retain(|_, portal| !portal.transaction_owned);
        self.unnamed_portal = None;
    }

    pub fn on_savepoint_rollback(&mut self, mark: PortalStoreMark) {
        self.named_portals.retain(|_, portal| {
            !(portal.transaction_owned && portal.created_generation > mark.generation)
        });
        if self
            .unnamed_portal
            .as_ref()
            .is_some_and(|portal| portal.created_generation > mark.generation)
        {
            self.unnamed_portal = None;
        }
    }

    fn remove_portals_for_statement_ref(&mut self, statement_ref: &PortalStatementRef) {
        self.named_portals
            .retain(|_, portal| &portal.statement_ref != statement_ref);
        if self
            .unnamed_portal
            .as_ref()
            .is_some_and(|portal| &portal.statement_ref == statement_ref)
        {
            self.unnamed_portal = None;
        }
    }
}

pub fn parameter_types_to_pg_array(parameter_types: &[Option<LogicalType>]) -> String {
    values_to_text(parameter_types)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prepared::portal::{
        CursorHoldability, FormatCode, PortalExecutionState, ScrollMode,
    };
    use paro_parser::ast::Statement;

    fn parse_single(sql: &str) -> Statement {
        paro_parser::parse(sql)
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
            .stmt
    }

    fn make_statement(name: &str) -> PreparedStatementEntry {
        PreparedStatementEntry {
            name: name.to_string(),
            source_sql: "SELECT 1".to_string(),
            raw_stmt: parse_single("SELECT 1"),
            parameter_types: Vec::new(),
            result_schema: Vec::new(),
            generic_plan: None,
            generic_plan_uses: 0,
            source: PreparedStatementSource::Protocol,
        }
    }

    fn make_portal(
        name: &str,
        holdability: CursorHoldability,
        transaction_owned: bool,
    ) -> PortalEntry {
        PortalEntry {
            name: name.to_string(),
            statement_ref: PortalStatementRef::None,
            source_sql: "SELECT 1".to_string(),
            raw_stmt: parse_single("SELECT 1"),
            holdability,
            scroll_mode: ScrollMode::Scroll,
            result_formats: vec![FormatCode::Text],
            result_schema: Vec::new(),
            kind: PortalKind::ClientCopy {
                stmt: Box::new(parse_single("SELECT 1")),
                parameter_env: TypedParameterEnv::default(),
            },
            execution_state: PortalExecutionState::Ready,
            snapshot_retention: None,
            completion: None,
            created_generation: 0,
            transaction_owned,
        }
    }

    #[test]
    fn unnamed_statement_replaces_previous_entry() {
        let mut state = PreparedState::default();

        assert!(state.set_unnamed_statement(make_statement("s1")).is_none());
        let replaced = state
            .set_unnamed_statement(make_statement("s2"))
            .expect("previous unnamed statement should be returned");

        assert_eq!(replaced.source_sql, "SELECT 1");
        assert!(state.unnamed_statement().is_some());
        assert_eq!(state.unnamed_statement().unwrap().name, "");
        assert!(state.statements().next().is_none());
    }

    #[test]
    fn transaction_commit_cleans_transaction_owned_portals_and_unnamed_portal() {
        let mut state = PreparedState::default();
        state.put_portal(make_portal("c_txn", CursorHoldability::WithoutHold, true));
        state.put_portal(make_portal("c_hold", CursorHoldability::WithHold, true));
        state.set_unnamed_portal(make_portal("ignored", CursorHoldability::WithHold, true));

        state.on_transaction_commit();

        assert!(state.portal("c_txn").is_none());
        let hold = state
            .portal("c_hold")
            .expect("holdable portal should remain");
        assert!(!hold.transaction_owned);
        assert!(state.unnamed_portal().is_none());
    }

    #[test]
    fn savepoint_rollback_removes_newer_transaction_owned_portals_only() {
        let mut state = PreparedState::default();
        state.put_portal(make_portal("older", CursorHoldability::WithoutHold, true));
        let mark = state.current_portal_mark();

        state.put_portal(make_portal(
            "newer_txn",
            CursorHoldability::WithoutHold,
            true,
        ));
        state.put_portal(make_portal(
            "newer_session",
            CursorHoldability::WithHold,
            false,
        ));
        state.set_unnamed_portal(make_portal("ignored", CursorHoldability::WithoutHold, true));

        state.on_savepoint_rollback(mark);

        assert!(state.portal("older").is_some());
        assert!(state.portal("newer_txn").is_none());
        assert!(state.portal("newer_session").is_some());
        assert!(state.unnamed_portal().is_none());
    }

    #[test]
    fn removing_statement_cascades_to_dependent_portals() {
        let mut state = PreparedState::default();
        state.insert_statement(make_statement("stmt1"));
        let mut portal = make_portal("p1", CursorHoldability::WithoutHold, false);
        portal.statement_ref = PortalStatementRef::Named("stmt1".to_string());
        state.put_portal(portal);

        let removed = state.remove_statement("stmt1");

        assert!(removed.is_some());
        assert!(state.portal("p1").is_none());
    }

    #[test]
    fn clearing_protocol_unnamed_objects_keeps_sql_named_state() {
        let mut state = PreparedState::default();
        state.insert_statement(PreparedStatementEntry {
            source: PreparedStatementSource::Sql,
            ..make_statement("stmt_sql")
        });
        state.set_unnamed_statement(make_statement(""));
        let mut portal = make_portal("named_portal", CursorHoldability::WithoutHold, false);
        portal.statement_ref = PortalStatementRef::Unnamed;
        state.put_portal(portal);
        state.set_unnamed_portal(make_portal(
            "ignored",
            CursorHoldability::WithoutHold,
            false,
        ));

        let changed = state.clear_protocol_unnamed_objects();

        assert!(changed);
        assert!(state.unnamed_statement().is_none());
        assert!(state.unnamed_portal().is_none());
        assert!(state.statement("stmt_sql").is_some());
        assert!(state.portal("named_portal").is_none());
    }
}
