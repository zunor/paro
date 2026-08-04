// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Binder Implementation
//!
//! The Binder converts semantic-less AST nodes into semantic-aware bound nodes.
//!
//! Layout: `context` (scopes), `ir` (bound tree), `bind` (AST → IR), `plan` (IR → logical operators).

pub mod bind;
pub mod context;
pub mod deep_copy;
pub mod ir;
pub mod plan;
#[cfg(test)]
pub(crate) mod test_utils;

use crate::binder::bind::clause::BoundGroupInformation;
use crate::binder::context::BindContext;
use crate::binder::ir::from::BoundFromItem;
use crate::binder::ir::BoundStatementKind;
use crate::expression::{Expression, ParameterExpression};
use crate::operator::LogicalOperator;
use crate::plan::{LogicalPlan, PlannedStatement};
use crate::stack::maybe_grow_planner_stack;
use paro_catalog::database_catalog::ParoCatalog;
use paro_catalog::entry::{CatalogObjectId, Dependency, DependencyList, DependencyType};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::error::{self as paro_error, Result};
use paro_common::typed_parameters::{ParameterSlot, RuntimeParamId};
use paro_common::types::LogicalType;
use paro_context::StatementContext;
use paro_function::scalar::cast::CastFunctionSet;
use paro_parser::ast::{Statement as AstStatement, TableReference};
use paro_parser::{Range, Span};
use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};

/// Information about a column referenced from an outer scope.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CorrelatedColumnInfo {
    pub table_index: usize,
    pub column_index: usize,
    pub return_type: paro_common::types::LogicalType,
    pub name: String,
    pub depth: usize,
}

/// Active GROUPING() binding context for the current SELECT binding scope.
#[derive(Debug, Clone, Default)]
pub struct GroupingBindingContext {
    /// Table index for GROUPING() output references.
    pub groupings_index: usize,
    /// Bound GROUP BY expression lookup info.
    pub group_info: BoundGroupInformation,
    /// GROUPING() function argument index lists collected during binding.
    pub grouping_functions: Vec<Vec<usize>>,
}

impl GroupingBindingContext {
    pub fn new(groupings_index: usize, group_info: BoundGroupInformation) -> Self {
        Self {
            groupings_index,
            group_info,
            grouping_functions: Vec::new(),
        }
    }
}

/// The Binder is responsible for converting semantic-less AST nodes into semantic-aware Bound nodes.
///
/// # Arc-based Design
///
/// Uses `Arc<StatementContext>` to avoid lifetime pollution throughout the codebase.
#[derive(Clone)]
pub struct Binder {
    /// Binding context for current scope (tables, aliases, column bindings).
    pub bind_context: BindContext,
    /// Session context for accessing catalog, transaction, and settings.
    session_context: Arc<StatementContext>,
    /// Cast functions registry.
    pub cast_functions: Arc<CastFunctionSet>,
    /// Columns referenced from outer scopes.
    pub correlated_columns: Vec<CorrelatedColumnInfo>,
    /// Table bindings that require virtual rowid column.
    row_id_bindings: HashSet<usize>,
    /// Active context used to bind GROUPING() calls inside SELECT/HAVING/QUALIFY.
    pub active_grouping_context: Option<GroupingBindingContext>,
    /// Whether planner-side delayed subquery planning is active for this binder.
    delayed_subquery_planning_enabled: bool,
    /// Optional dependency collector used by CREATE VIEW-style binders.
    dependency_collector: Option<Arc<Mutex<BTreeMap<CatalogObjectId, Dependency>>>>,
    /// Optional type signature for protocol placeholders.
    parameter_types: Option<Arc<[LogicalType]>>,
    /// Stable placeholder ordering keyed by parser span.
    placeholder_indexes: Option<Arc<BTreeMap<Range, usize>>>,
}

impl Binder {
    /// Create a new Binder with the given session context.
    pub fn new(session_context: Arc<StatementContext>) -> Self {
        let cast_functions = session_context.cast_functions();

        Self {
            bind_context: BindContext::new(),
            session_context,
            cast_functions,
            correlated_columns: Vec::new(),
            row_id_bindings: HashSet::new(),
            active_grouping_context: None,
            delayed_subquery_planning_enabled: true,
            dependency_collector: None,
            parameter_types: None,
            placeholder_indexes: None,
        }
    }

    pub fn with_parameters(
        session_context: Arc<StatementContext>,
        parameter_types: Vec<LogicalType>,
        placeholder_indexes: BTreeMap<Range, usize>,
    ) -> Self {
        let cast_functions = session_context.cast_functions();

        Self {
            bind_context: BindContext::new(),
            session_context,
            cast_functions,
            correlated_columns: Vec::new(),
            row_id_bindings: HashSet::new(),
            active_grouping_context: None,
            delayed_subquery_planning_enabled: true,
            dependency_collector: None,
            parameter_types: Some(Arc::from(parameter_types.into_boxed_slice())),
            placeholder_indexes: Some(Arc::new(placeholder_indexes)),
        }
    }

    /// Get the catalog from the session context.
    #[inline]
    pub fn catalog(&self) -> Arc<ParoCatalog> {
        self.session_context.catalog()
    }

    /// Get the catalog transaction from the session context.
    #[inline]
    pub fn catalog_txn_view(&self) -> CatalogSnapshot {
        self.session_context.catalog_txn_view()
    }

    /// Get the session context reference.
    #[inline]
    pub fn session_context(&self) -> &StatementContext {
        self.session_context.as_ref()
    }

    /// Wrap a logical operator as a child [`LogicalPlan`] using the current bind context.
    #[inline]
    pub(crate) fn wrap_plan(&self, op: LogicalOperator) -> crate::plan::LogicalPlan {
        crate::plan::LogicalPlan::new(&self.bind_context, op)
    }

    /// Create a child binder for nested scopes (e.g., subqueries).
    pub fn create_child(&self) -> Binder {
        Binder {
            bind_context: self.bind_context.create_child(),
            session_context: self.session_context.clone(),
            cast_functions: Arc::clone(&self.cast_functions),
            correlated_columns: Vec::new(),
            row_id_bindings: HashSet::new(),
            active_grouping_context: None,
            delayed_subquery_planning_enabled: self.delayed_subquery_planning_enabled,
            dependency_collector: self.dependency_collector.clone(),
            parameter_types: self.parameter_types.clone(),
            placeholder_indexes: self.placeholder_indexes.clone(),
        }
    }

    pub fn create_child_without_dependency_collection(&self) -> Binder {
        let mut child = self.create_child();
        child.dependency_collector = None;
        child
    }

    pub fn enable_dependency_collection(&mut self) {
        self.dependency_collector = Some(Arc::new(Mutex::new(BTreeMap::new())));
    }

    pub fn record_dependency(&self, dependency: Dependency) {
        let Some(collector) = &self.dependency_collector else {
            return;
        };
        if let Ok(mut collector) = collector.lock() {
            collector.insert(dependency.entry.id, dependency);
        }
    }

    pub fn record_regular_dependency(&self, entry: paro_catalog::entry::CatalogObjectRef) {
        self.record_dependency(Dependency::new(entry, DependencyType::Regular));
    }

    pub fn collected_dependencies(&self) -> DependencyList {
        let Some(collector) = &self.dependency_collector else {
            return DependencyList::new();
        };
        let Ok(collector) = collector.lock() else {
            return DependencyList::new();
        };
        let mut dependencies = DependencyList::new();
        for dependency in collector.values() {
            dependencies.add_dependency(dependency.entry.clone(), dependency.dependency_type);
        }
        dependencies
    }

    /// Run a closure with a temporary GROUPING() binding context, restoring the
    /// previous context afterwards.
    pub fn with_grouping_context<T, F>(
        &mut self,
        context: Option<GroupingBindingContext>,
        f: F,
    ) -> T
    where
        F: FnOnce(&mut Self) -> T,
    {
        let previous = self.active_grouping_context.take();
        self.active_grouping_context = context;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(self)));
        let _ = self.active_grouping_context.take();
        self.active_grouping_context = previous;
        match result {
            Ok(value) => value,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }

    pub fn with_overlay_scope<T, F>(&mut self, f: F) -> T
    where
        F: FnOnce(&mut Self) -> T,
    {
        self.bind_context.push_overlay_frame();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(self)));
        self.bind_context.pop_overlay_frame();
        match result {
            Ok(value) => value,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }

    /// Mark a table binding as requiring virtual rowid output.
    pub fn mark_row_id_binding(&mut self, table_index: usize) {
        self.row_id_bindings.insert(table_index);
    }

    /// Check whether a table binding needs virtual rowid output.
    pub fn needs_row_id_binding(&self, table_index: usize) -> bool {
        self.row_id_bindings.contains(&table_index)
    }

    fn protocol_parameter_at_span(&self, span: Span) -> Result<(usize, &LogicalType)> {
        let span = span.ok_or_else(|| {
            paro_error::protocol_violation(
                "parameterized placeholder is missing parser span metadata".to_string(),
            )
        })?;
        let placeholder_indexes = self.placeholder_indexes.as_ref().ok_or_else(|| {
            paro_error::protocol_violation(
                "placeholder index map is not available for parameterized compilation".to_string(),
            )
        })?;
        let parameter_types = self.parameter_types.as_ref().ok_or_else(|| {
            paro_error::protocol_violation(
                "parameter type signature is not available for parameterized compilation"
                    .to_string(),
            )
        })?;
        let Some(index) = placeholder_indexes.get(&span).copied() else {
            return Err(paro_error::protocol_violation(format!(
                "unbound placeholder at span {span}",
            )));
        };
        parameter_types
            .get(index)
            .map(|logical_type| (index, logical_type))
            .ok_or_else(|| {
                paro_error::protocol_violation(format!(
                    "expected at least {} bound parameters, got {}",
                    index + 1,
                    parameter_types.len(),
                ))
            })
    }

    pub fn bind_protocol_parameter(&self, span: Span) -> Result<Expression> {
        let span = span.ok_or_else(|| {
            paro_error::protocol_violation(
                "parameterized placeholder is missing parser span metadata".to_string(),
            )
        })?;
        let (index, logical_type) = self.protocol_parameter_at_span(Some(span))?;
        Ok(Expression::Parameter(ParameterExpression::new(
            ParameterSlot::new(RuntimeParamId::new(index), logical_type.clone()),
        )))
    }

    pub(crate) fn delayed_subquery_planning_enabled(&self) -> bool {
        self.delayed_subquery_planning_enabled
    }

    pub(crate) fn with_delayed_subquery_planning_disabled<T, F>(&mut self, f: F) -> T
    where
        F: FnOnce(&mut Self) -> T,
    {
        let previous = self.delayed_subquery_planning_enabled;
        self.delayed_subquery_planning_enabled = false;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(self)));
        self.delayed_subquery_planning_enabled = previous;
        match result {
            Ok(value) => value,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }

    pub fn bind_statement_kind(&mut self, statement: AstStatement) -> Result<BoundStatementKind> {
        match statement {
            AstStatement::CreateTable(create_table) => {
                bind::statement::create_table::bind_create_table(self, create_table)
            }
            AstStatement::CreateFunction(stmt) => {
                bind::statement::create_function::bind_create_function(self, stmt)
            }
            AstStatement::CreateSequence(stmt) => {
                bind::statement::create_sequence::bind_create_sequence(self, stmt)
            }
            AstStatement::CreatePropertyGraph(stmt) => {
                bind::statement::create_property_graph::bind_create_property_graph(self, stmt)
            }
            AstStatement::CreateSchema(create_schema) => {
                bind::statement::create_schema::bind_create_schema(self, create_schema)
            }
            AstStatement::CreateIndex(stmt) => {
                bind::statement::create_index::bind_create_index(self, stmt)
            }
            AstStatement::CreateAggregatingIndex(_) => Err(paro_common::error::not_supported(
                "AGGREGATING INDEX is not yet implemented".to_string(),
            )),
            AstStatement::CreateView(create_view) => {
                let bound_info = bind::statement::create_view::bind_create_view(self, create_view)?;
                Ok(BoundStatementKind::CreateView(bound_info))
            }
            AstStatement::Insert(insert) => bind::statement::insert::bind_insert(self, insert),
            AstStatement::Delete(delete) => bind::statement::delete::bind_delete(self, delete),
            AstStatement::Update(update) => bind::statement::update::bind_update(self, update),
            AstStatement::DropSchema(drop_schema) => {
                bind::statement::drop::bind_drop_schema(self, drop_schema)
            }
            AstStatement::DropTable(drop_table) => {
                bind::statement::drop::bind_drop_table(self, drop_table)
            }
            AstStatement::DropIndex(drop_index) => {
                bind::statement::drop::bind_drop_index(self, drop_index)
            }
            AstStatement::DropView(drop_view) => {
                bind::statement::drop::bind_drop_view(self, drop_view)
            }
            AstStatement::DropFunction(stmt) => {
                bind::statement::drop_function::bind_drop_function(self, stmt)
            }
            AstStatement::DropSequence(drop_sequence) => {
                bind::statement::drop::bind_drop_sequence(self, drop_sequence)
            }
            AstStatement::RenameTable(rename_table) => {
                bind::statement::alter::bind_rename_table(self, rename_table)
            }
            AstStatement::AlterTable(stmt) => bind::statement::alter::bind_alter_table(self, stmt),
            AstStatement::DropPropertyGraph(stmt) => {
                bind::statement::drop_property_graph::bind_drop_property_graph(self, stmt)
            }
            AstStatement::RefreshPropertyGraph(stmt) => {
                bind::statement::refresh_property_graph::bind_refresh_property_graph(self, stmt)
            }
            AstStatement::Explain {
                kind,
                options,
                query,
            } => bind::statement::explain::bind_explain(self, *query, kind, options),
            AstStatement::ExplainAnalyze { query, .. } => {
                bind::statement::explain::bind_explain_analyze(self, *query)
            }
            AstStatement::Query(query) => bind::statement::select::bind_select(self, *query),
            AstStatement::Copy(copy) => bind::statement::copy::bind_copy(self, copy),
            _ => Err(paro_common::error::not_implemented(format!(
                "Bind Statement: {:?}",
                statement
            ))),
        }
    }

    /// Bind a SQL statement.
    pub fn bind(&mut self, statement: AstStatement) -> Result<PlannedStatement> {
        maybe_grow_planner_stack(|| self.bind_inner(statement))
    }

    fn bind_inner(&mut self, statement: AstStatement) -> Result<PlannedStatement> {
        let bound_statement = self.bind_statement_kind(statement)?;

        let names = bound_statement.names();
        let types = bound_statement.types();
        let plan = self.create_plan(bound_statement)?;

        Ok(PlannedStatement { types, names, plan })
    }

    pub fn create_plan(&mut self, statement: BoundStatementKind) -> Result<LogicalPlan> {
        maybe_grow_planner_stack(|| self.create_plan_inner(statement))
    }

    fn create_plan_inner(&mut self, statement: BoundStatementKind) -> Result<LogicalPlan> {
        let operator = match statement {
            BoundStatementKind::Query(node) => self.plan_query(*node),
            BoundStatementKind::Insert(info) => self.plan_insert(info),
            BoundStatementKind::CreateTable(info) => self.plan_create_table(info),
            BoundStatementKind::CreateRoutine(info) => self.plan_create_routine(info),
            BoundStatementKind::CreateSequence(info) => self.plan_create_sequence(info),
            BoundStatementKind::CreateSchema(info) => self.plan_create_schema(info),
            BoundStatementKind::CreateIndex(info) => self.plan_create_index(info),
            BoundStatementKind::CreatePropertyGraph(info) => self.plan_create_property_graph(info),
            BoundStatementKind::CreateView(info) => self.plan_create_view(info),
            BoundStatementKind::AlterEntry(info) => self.plan_alter_entry(info),
            BoundStatementKind::Explain(info) => Ok(info.plan),
            BoundStatementKind::Copy(info) => Ok(info.plan),
            BoundStatementKind::Delete(info) => self.plan_delete(info),
            BoundStatementKind::Update(info) => self.plan_update(info),
            BoundStatementKind::Drop(info) => self.plan_drop(info),
            BoundStatementKind::DropPropertyGraph(info) => self.plan_drop_property_graph(info),
            BoundStatementKind::RefreshPropertyGraph(info) => {
                self.plan_refresh_property_graph(info)
            }
            BoundStatementKind::Dummy => Err(paro_common::error::not_implemented(
                "Planning for statement: Dummy",
            )),
        }?;
        Ok(LogicalPlan::new(&self.bind_context, operator))
    }

    /// Bind a table reference (FROM clause).
    pub fn bind_table_ref(&mut self, table_ref: TableReference) -> Result<BoundFromItem> {
        match table_ref {
            TableReference::Table {
                database,
                schema,
                table,
                alias,
                ..
            } => bind::from::base_table::bind_base_table(self, database, schema, table, alias),
            TableReference::TableFunction {
                lateral,
                name,
                params,
                named_params,
                alias,
                with_ordinality,
                ..
            } => {
                let function_name = name.name.clone();
                bind::from::table_function::bind_table_function_ref(
                    self,
                    &function_name,
                    params,
                    named_params,
                    alias,
                    lateral,
                    with_ordinality,
                )
            }
            TableReference::Join { join, .. } => bind::from::join::bind_join(self, join),
            TableReference::Subquery {
                subquery,
                alias,
                lateral,
                ..
            } => bind::from::subquery::bind_subquery_ref(self, *subquery, alias, lateral),
            TableReference::Location { .. } => Err(paro_common::error::not_implemented(
                "TableReference::Location",
            )),
            TableReference::GraphTable {
                graph_table, alias, ..
            } => bind::from::graph_table::bind_graph_table(self, graph_table, alias),
        }
    }

    pub fn flatten_dependent_joins(&mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        plan::subquery::flatten_all_dependent_joins(self, plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::bind::expr::bind_expression;
    use crate::binder::ir::{BoundQuery, BoundStatementKind};
    use crate::binder::test_utils::{test_binder, test_binder_with_public_table};
    use paro_catalog::entry::{AlterEntryAction, CatalogType};
    use paro_common::types::LogicalType;

    fn parse_expr_sql(sql: &str) -> paro_parser::ast::Expr {
        let tokens = paro_parser::tokenize_sql(sql).expect("tokenize");
        paro_parser::parse_expr_tokens(&tokens).expect("parse expr")
    }

    fn parse_statement_sql(sql: &str) -> paro_parser::ast::Statement {
        paro_parser::parse_one(sql).expect("parse sql").stmt
    }

    #[test]
    fn with_overlay_scope_hides_temporary_bindings_after_return() {
        let mut binder = test_binder();
        binder.bind_context.add_binding(
            "base".to_string(),
            1,
            vec!["x".to_string()],
            vec![paro_common::types::LogicalType::Integer],
        );

        let bound = binder.with_overlay_scope(|binder| {
            binder.bind_context.add_binding(
                "temp".to_string(),
                2,
                vec!["y".to_string()],
                vec![paro_common::types::LogicalType::Integer],
            );
            bind_expression(binder, parse_expr_sql("temp.y")).expect("bind temp column")
        });

        assert!(matches!(bound, crate::expression::Expression::ColumnRef(_)));
        assert!(binder.bind_context.lookup_binding("temp").is_none());
        assert!(binder.bind_context.lookup_binding("base").is_some());
    }

    #[test]
    fn overlay_scope_snapshot_is_visible_to_nested_child_binders() {
        let mut binder = test_binder();
        binder.bind_context.add_binding(
            "base".to_string(),
            1,
            vec!["x".to_string()],
            vec![paro_common::types::LogicalType::Integer],
        );

        let bound = binder.with_overlay_scope(|binder| {
            binder.bind_context.add_binding(
                "temp".to_string(),
                2,
                vec!["y".to_string()],
                vec![paro_common::types::LogicalType::Integer],
            );

            let mut child = binder.create_child();
            bind_expression(&mut child, parse_expr_sql("temp.y")).expect("bind overlay column")
        });

        let crate::expression::Expression::ColumnRef(column_ref) = bound else {
            panic!("expected correlated column ref");
        };
        assert_eq!(column_ref.binding.table_index, 2);
        assert_eq!(column_ref.binding.column_index, 0);
        assert_eq!(column_ref.depth, 1);

        let err = bind_expression(&mut binder, parse_expr_sql("temp.y"))
            .expect_err("overlay binding should be gone after the scope closes");
        assert!(err.to_string().contains("Column not found: temp.y"));
    }

    #[test]
    fn temporary_state_helpers_restore_state_after_panic() {
        let mut binder = test_binder();
        let grouping_context = Some(GroupingBindingContext::new(
            9,
            BoundGroupInformation::default(),
        ));

        let overlay = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            binder.with_overlay_scope(|binder| {
                binder.bind_context.add_binding(
                    "temp".to_string(),
                    2,
                    vec!["y".to_string()],
                    vec![paro_common::types::LogicalType::Integer],
                );
                panic!("overlay panic");
            });
        }));
        assert!(overlay.is_err());
        assert!(binder.bind_context.lookup_binding("temp").is_none());

        let grouping = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            binder.with_grouping_context(grouping_context.clone(), |_binder| {
                panic!("grouping panic");
            });
        }));
        assert!(grouping.is_err());
        assert!(binder.active_grouping_context.is_none());

        let delayed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            binder.with_delayed_subquery_planning_disabled(|binder| {
                assert!(!binder.delayed_subquery_planning_enabled());
                panic!("delayed panic");
            });
        }));
        assert!(delayed.is_err());
        assert!(binder.delayed_subquery_planning_enabled());
    }

    #[test]
    fn where_clause_can_reference_select_alias_via_early_lookup() {
        let mut binder = test_binder();
        let bound = binder
            .bind_statement_kind(parse_statement_sql(
                "SELECT x AS y FROM (SELECT 1 AS x) t WHERE y = 1",
            ))
            .expect("bind statement");

        let BoundStatementKind::Query(query) = bound else {
            panic!("expected query statement");
        };
        let BoundQuery::Select(select) = *query else {
            panic!("expected bound select");
        };

        assert!(matches!(
            select.where_clause,
            Some(crate::expression::Expression::Comparison(_))
        ));
    }

    #[test]
    fn where_clause_does_not_lowercase_match_quoted_aliases() {
        let mut binder = test_binder();
        let err = binder
            .bind_statement_kind(parse_statement_sql(
                "SELECT x AS \"ID\" FROM (SELECT 1 AS x) t WHERE id = 1",
            ))
            .expect_err("quoted alias should require exact spelling");

        assert!(err.to_string().contains("Column not found: id"));
    }

    #[test]
    fn rename_table_binds_to_alter_entry() {
        let mut binder = test_binder_with_public_table(
            "users",
            &[
                ("id", LogicalType::Integer),
                ("old_id", LogicalType::Integer),
            ],
        );
        let bound = binder
            .bind_statement_kind(parse_statement_sql("RENAME TABLE users TO users_v2"))
            .expect("bind rename table");

        let BoundStatementKind::AlterEntry(info) = bound else {
            panic!("expected alter entry");
        };
        assert_eq!(info.info.entry_type, CatalogType::Table);
        assert_eq!(info.info.name, "users");
        match &info.info.action {
            AlterEntryAction::Move {
                new_name,
                new_schema,
            } => {
                assert_eq!(new_name, "users_v2");
                assert!(new_schema.is_none());
            }
            other => panic!("expected move action, got {other:?}"),
        }
    }

    #[test]
    fn alter_table_rename_table_binds_to_alter_entry() {
        let mut binder = test_binder_with_public_table(
            "users",
            &[
                ("id", LogicalType::Integer),
                ("old_id", LogicalType::Integer),
            ],
        );
        let bound = binder
            .bind_statement_kind(parse_statement_sql("ALTER TABLE users RENAME TO users_v2"))
            .expect("bind alter table rename table");

        let BoundStatementKind::AlterEntry(info) = bound else {
            panic!("expected alter entry");
        };
        assert_eq!(info.info.entry_type, CatalogType::Table);
        assert_eq!(info.info.name, "users");
        match &info.info.action {
            AlterEntryAction::Move {
                new_name,
                new_schema,
            } => {
                assert_eq!(new_name, "users_v2");
                assert!(new_schema.is_none());
            }
            other => panic!("expected move action, got {other:?}"),
        }
    }

    #[test]
    fn alter_table_rename_column_binds_to_alter_entry() {
        let mut binder = test_binder_with_public_table(
            "users",
            &[
                ("id", LogicalType::Integer),
                ("old_id", LogicalType::Integer),
            ],
        );
        let bound = binder
            .bind_statement_kind(parse_statement_sql(
                "ALTER TABLE users RENAME COLUMN old_id TO new_id",
            ))
            .expect("bind alter table rename column");

        let BoundStatementKind::AlterEntry(info) = bound else {
            panic!("expected alter entry");
        };
        assert_eq!(info.info.entry_type, CatalogType::Table);
        assert_eq!(info.info.name, "users");
        match &info.info.action {
            AlterEntryAction::RenameColumn {
                old_column_name,
                new_column_name,
            } => {
                assert_eq!(old_column_name, "old_id");
                assert_eq!(new_column_name, "new_id");
            }
            other => panic!("expected rename column action, got {other:?}"),
        }
    }

    #[test]
    fn comment_on_column_binds_to_alter_entry() {
        let mut binder = test_binder_with_public_table(
            "users",
            &[
                ("id", LogicalType::Integer),
                ("old_id", LogicalType::Integer),
            ],
        );
        let bound = binder
            .bind_statement_kind(parse_statement_sql(
                "COMMENT ON COLUMN users.id IS 'pk column'",
            ))
            .expect("bind comment on column");

        let BoundStatementKind::AlterEntry(info) = bound else {
            panic!("expected alter entry");
        };
        assert_eq!(info.info.entry_type, CatalogType::Table);
        assert_eq!(info.info.name, "users");
        match &info.info.action {
            AlterEntryAction::SetColumnComments { comments } => {
                assert_eq!(comments.len(), 1);
                assert_eq!(comments[0].column_name, "id");
                assert_eq!(comments[0].comment, "pk column");
            }
            other => panic!("expected column comment action, got {other:?}"),
        }
    }

    #[test]
    fn rename_table_across_schema_binds_target_schema() {
        let mut binder = test_binder_with_public_table(
            "users",
            &[
                ("id", LogicalType::Integer),
                ("old_id", LogicalType::Integer),
            ],
        );
        let bound = binder
            .bind_statement_kind(parse_statement_sql(
                "RENAME TABLE public.users TO archive.users_v2",
            ))
            .expect("bind cross-schema rename table");

        let BoundStatementKind::AlterEntry(info) = bound else {
            panic!("expected alter entry");
        };
        assert_eq!(info.info.entry_type, CatalogType::Table);
        assert_eq!(info.info.name, "users");
        match &info.info.action {
            AlterEntryAction::Move {
                new_name,
                new_schema,
            } => {
                assert_eq!(new_name, "users_v2");
                assert_eq!(new_schema.as_deref(), Some("archive"));
            }
            other => panic!("expected move action, got {other:?}"),
        }
    }
}
