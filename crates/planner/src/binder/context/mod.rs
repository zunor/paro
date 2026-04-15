// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Binding context layer (`BindShared`, `ScopeFrame`, snapshots).

mod frame;
mod shared;
mod snapshot;

pub use frame::{Binding, ScopeFrame};
pub use shared::BindShared;
pub use snapshot::BindSnapshot;

use crate::binder::ir::CTEBindState;
use crate::plan::PlanNodeId;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_parser::ast::{ColumnID, ColumnRef, Expr, Identifier};
use std::mem;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnLookup {
    pub alias: String,
    pub table_index: usize,
    pub column_index: usize,
    pub return_type: LogicalType,
    pub depth: usize,
}

/// BindContext manages the scope of bindings (tables, aliases) during query binding.
#[derive(Debug, Clone)]
pub struct BindContext {
    shared: Arc<BindShared>,
    current_frame: ScopeFrame,
    overlay_frames: Vec<ScopeFrame>,
    parent: Option<Arc<BindSnapshot>>,
    unnamed_subquery_count: usize,
}

impl Default for BindContext {
    fn default() -> Self {
        Self::new()
    }
}

impl BindContext {
    pub fn new() -> Self {
        Self {
            shared: Arc::new(BindShared::new()),
            current_frame: ScopeFrame::default(),
            overlay_frames: Vec::new(),
            parent: None,
            unnamed_subquery_count: 0,
        }
    }

    pub fn from_shared(shared: Arc<BindShared>) -> Self {
        Self {
            shared,
            current_frame: ScopeFrame::default(),
            overlay_frames: Vec::new(),
            parent: None,
            unnamed_subquery_count: 0,
        }
    }

    pub fn create_child(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            current_frame: ScopeFrame::default(),
            overlay_frames: Vec::new(),
            parent: Some(self.snapshot()),
            unnamed_subquery_count: 0,
        }
    }

    pub fn snapshot(&self) -> Arc<BindSnapshot> {
        let frame = self.visible_frame();
        Arc::new(BindSnapshot::new(
            Arc::clone(&self.shared),
            frame,
            self.parent.clone(),
        ))
    }

    pub fn shared(&self) -> &Arc<BindShared> {
        &self.shared
    }

    pub fn generate_table_index(&self) -> usize {
        self.shared.generate_table_index()
    }

    pub fn next_plan_id(&self) -> PlanNodeId {
        self.shared.next_plan_id()
    }

    pub fn add_binding(
        &mut self,
        alias: String,
        index: usize,
        column_names: Vec<String>,
        column_types: Vec<LogicalType>,
    ) {
        self.current_frame.upsert_binding(Binding {
            alias,
            index,
            column_names,
            column_types,
        });
    }

    pub fn remove_binding(&mut self, alias: &str) -> Option<Binding> {
        self.current_frame.remove_binding(alias)
    }

    pub fn remove_bindings_by_index(&mut self, indices: &[usize]) {
        if indices.is_empty() {
            return;
        }

        self.current_frame
            .retain_bindings(|_, binding| !indices.contains(&binding.index));
    }

    pub fn iter_bindings(&self) -> impl Iterator<Item = &Binding> {
        self.current_frame.bindings()
    }

    pub fn iter_bindings_ordered(&self) -> Vec<&Binding> {
        let mut bindings: Vec<_> = self.iter_bindings().collect();
        bindings.sort_by_key(|binding| binding.index);
        bindings
    }

    pub fn lookup_binding(&self, alias: &str) -> Option<&Binding> {
        self.current_frame.get_binding(alias)
    }

    pub fn find_binding_by_index(&self, index: usize) -> Option<&Binding> {
        self.current_visible_bindings()
            .find(|binding| binding.index == index)
    }

    pub fn lookup_local_column(
        &self,
        table_name: Option<&str>,
        column_name: &str,
    ) -> Result<Option<ColumnLookup>> {
        let mut found = None;

        for binding in self.current_visible_bindings() {
            if let Some(target_table) = table_name {
                if binding.alias != target_table {
                    continue;
                }
            }

            if let Some(column_index) = binding.column_names.iter().position(|c| c == column_name) {
                if found.is_some() {
                    return Err(paro_error::catalog(format!(
                        "Ambiguous column name: {}",
                        column_name
                    )));
                }

                found = Some(ColumnLookup {
                    alias: binding.alias.clone(),
                    table_index: binding.index,
                    column_index,
                    return_type: binding.column_types[column_index].clone(),
                    depth: 0,
                });
            }
        }

        Ok(found)
    }

    pub fn lookup_outer_column(
        &self,
        table_name: Option<&str>,
        column_name: &str,
    ) -> Result<Option<ColumnLookup>> {
        match self.parent.as_deref() {
            Some(parent) => parent.lookup_column(table_name, column_name, 1),
            None => Ok(None),
        }
    }

    pub fn resolve_unqualified_column(&self, column_name: &str) -> Result<Option<ColumnLookup>> {
        self.lookup_local_column(None, column_name)
    }

    pub fn lookup_cte(&self, name: &str) -> Option<Arc<CTEBindState>> {
        self.get_cte(name)
    }

    /// Generates expressions for all columns in the current scope.
    /// Used for `SELECT *` expansion.
    /// If `relation_name` is provided (e.g., `SELECT table.*`), only returns columns from that table.
    pub fn generate_all_column_expressions(&self, relation_name: Option<&str>) -> Vec<Expr> {
        let mut exprs = Vec::new();

        for binding in self.iter_bindings_ordered() {
            if let Some(target_alias) = relation_name {
                if binding.alias != target_alias {
                    continue;
                }
            }

            for col_name in &binding.column_names {
                exprs.push(Expr::ColumnRef {
                    span: paro_parser::Span::default(),
                    column: ColumnRef {
                        schema: None,
                        table: Some(Identifier::from_name(
                            paro_parser::Span::default(),
                            binding.alias.clone(),
                        )),
                        column: ColumnID::Name(Identifier::from_name(
                            paro_parser::Span::default(),
                            col_name.clone(),
                        )),
                    },
                });
            }
        }

        exprs
    }

    /// Register a CTE (Common Table Expression) in this context.
    /// The CTE is not bound yet - it will be lazily bound when referenced.
    pub fn register_cte(&mut self, name: String, info: Arc<CTEBindState>) {
        self.current_frame.insert_cte(name, info);
    }

    /// Look up a CTE by name in this context or parent snapshots.
    pub fn get_cte(&self, name: &str) -> Option<Arc<CTEBindState>> {
        if let Some(info) = self
            .current_visible_frames()
            .find_map(|frame| frame.get_cte(name))
        {
            return Some(Arc::clone(info));
        }
        self.parent.as_ref().and_then(|parent| parent.get_cte(name))
    }

    pub fn has_cte(&self, name: &str) -> bool {
        self.get_cte(name).is_some()
    }

    pub fn has_parent(&self) -> bool {
        self.parent.is_some()
    }

    pub fn parent_snapshot(&self) -> Option<&BindSnapshot> {
        self.parent.as_deref()
    }

    pub fn unnamed_subquery_count(&self) -> usize {
        self.unnamed_subquery_count
    }

    pub fn next_unnamed_subquery_alias(&mut self) -> String {
        let alias = format!("unnamed_subquery{}", self.unnamed_subquery_count);
        self.unnamed_subquery_count += 1;
        alias
    }

    pub(crate) fn push_overlay_frame(&mut self) {
        let previous = mem::take(&mut self.current_frame);
        self.overlay_frames.push(previous);
    }

    pub(crate) fn pop_overlay_frame(&mut self) {
        self.current_frame = self
            .overlay_frames
            .pop()
            .expect("pop_overlay_frame called without matching push");
    }

    fn current_visible_frames(&self) -> impl Iterator<Item = &ScopeFrame> {
        std::iter::once(&self.current_frame).chain(self.overlay_frames.iter().rev())
    }

    fn current_visible_bindings(&self) -> impl Iterator<Item = &Binding> {
        self.current_visible_frames()
            .flat_map(|frame| frame.bindings())
    }

    fn visible_frame(&self) -> ScopeFrame {
        if self.overlay_frames.is_empty() {
            return self.current_frame.clone();
        }

        // Overlay bindings are visible in the current lexical scope, so snapshots taken inside the
        // overlay intentionally freeze them as part of the parent scope for nested binders.
        let mut frame = self.overlay_frames.first().cloned().unwrap_or_default();

        for overlay in self.overlay_frames.iter().skip(1) {
            for (_, binding) in overlay.binding_entries() {
                frame.upsert_binding(binding.clone());
            }
            for (name, state) in overlay.cte_entries() {
                frame.insert_cte(name.clone(), Arc::clone(state));
            }
        }

        for (_, binding) in self.current_frame.binding_entries() {
            frame.upsert_binding(binding.clone());
        }
        for (name, state) in self.current_frame.cte_entries() {
            frame.insert_cte(name.clone(), Arc::clone(state));
        }

        frame
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::BindContext;
    use crate::binder::ir::{CTEBindInfo, CTEBindState, CTEMaterialize};
    use paro_common::types::LogicalType;
    use paro_parser::ast::Expr;

    #[test]
    fn snapshot_keeps_outer_scope_visible_after_current_context_changes() {
        let mut parent = BindContext::new();
        parent.add_binding(
            "base".to_string(),
            1,
            vec!["x".to_string()],
            vec![LogicalType::Integer],
        );

        let mut child = parent.create_child();
        child.add_binding(
            "child".to_string(),
            2,
            vec!["y".to_string()],
            vec![LogicalType::Integer],
        );

        parent.add_binding(
            "late".to_string(),
            3,
            vec!["z".to_string()],
            vec![LogicalType::Integer],
        );

        let outer = child
            .lookup_outer_column(Some("base"), "x")
            .expect("lookup")
            .expect("outer column");
        assert_eq!(outer.table_index, 1);

        let missing = child
            .lookup_outer_column(Some("late"), "z")
            .expect("lookup late");
        assert!(missing.is_none());
    }

    #[test]
    fn snapshot_is_copy_on_write_for_current_frame_and_ctes() {
        let mut context = BindContext::new();
        context.add_binding(
            "base".to_string(),
            1,
            vec!["x".to_string()],
            vec![LogicalType::Integer],
        );
        let base_query = match paro_parser::parse_one("SELECT 1 AS x")
            .expect("parse cte query")
            .stmt
        {
            paro_parser::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        };
        context.register_cte(
            "base_cte".to_string(),
            Arc::new(CTEBindState::new(CTEBindInfo {
                name: "base_cte".to_string(),
                aliases: vec!["x".to_string()],
                query: base_query,
                materialized: CTEMaterialize::Default,
                cte_index: 7,
                recursive: false,
            })),
        );

        let snapshot = context.snapshot();

        context.add_binding(
            "late".to_string(),
            2,
            vec!["y".to_string()],
            vec![LogicalType::Integer],
        );
        let late_query = match paro_parser::parse_one("SELECT 2 AS y")
            .expect("parse late cte query")
            .stmt
        {
            paro_parser::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        };
        context.register_cte(
            "late_cte".to_string(),
            Arc::new(CTEBindState::new(CTEBindInfo {
                name: "late_cte".to_string(),
                aliases: vec!["y".to_string()],
                query: late_query,
                materialized: CTEMaterialize::Default,
                cte_index: 8,
                recursive: false,
            })),
        );

        assert!(snapshot
            .lookup_column(Some("base"), "x", 0)
            .expect("lookup base column")
            .is_some());
        assert!(snapshot
            .lookup_column(Some("late"), "y", 0)
            .expect("lookup late column")
            .is_none());
        assert!(snapshot.get_cte("base_cte").is_some());
        assert!(snapshot.get_cte("late_cte").is_none());
    }

    #[test]
    fn generate_all_column_expressions_respects_order_and_relation_filter() {
        let mut context = BindContext::new();
        context.add_binding(
            "first".to_string(),
            1,
            vec!["a".to_string(), "b".to_string()],
            vec![LogicalType::Integer, LogicalType::Integer],
        );
        context.add_binding(
            "second".to_string(),
            2,
            vec!["c".to_string()],
            vec![LogicalType::Integer],
        );

        let all = context.generate_all_column_expressions(None);
        assert_eq!(all.len(), 3);

        let Expr::ColumnRef { column, .. } = &all[0] else {
            panic!("expected column ref");
        };
        assert_eq!(column.table.as_ref().expect("table").name, "first");
        assert_eq!(column.column.name(), "a");

        let Expr::ColumnRef { column, .. } = &all[2] else {
            panic!("expected column ref");
        };
        assert_eq!(column.table.as_ref().expect("table").name, "second");
        assert_eq!(column.column.name(), "c");

        let filtered = context.generate_all_column_expressions(Some("second"));
        assert_eq!(filtered.len(), 1);
        let Expr::ColumnRef { column, .. } = &filtered[0] else {
            panic!("expected column ref");
        };
        assert_eq!(column.table.as_ref().expect("table").name, "second");
        assert_eq!(column.column.name(), "c");
    }
}
