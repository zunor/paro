// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Bind SELECT statement body (`SelectStmt` → `BoundSelect`)
//!
//!
//!
//! 1. FROM clause (table references)
//! 2. Expand SELECT list (star expressions)
//! 3. Record column_count before ORDER BY additions
//! 4. WHERE clause (WhereBinder)
//! 5. PrepareModifiers - ORDER BY binding (may add expressions to SELECT list)
//! 6. GROUP BY clause (GroupBinder)
//! 7. SELECT list binding (includes any ORDER BY additions)
//! 8. HAVING clause (HavingBinder)
//! 9. QUALIFY clause (QualifyBinder)
//! 10. BindModifiers - finalize ORDER BY types

use crate::binder::bind::{
    clause::{
        AliasLookup, BoundGroupInformation, GroupBinder, HavingBinder, OrderBinder, OrderByBinding,
        QualifyBinder, SelectBindState, SelectBinder, WhereBinder,
    },
    expr::{self, ExpressionBinder},
};
use crate::binder::ir::{
    BoundFromItem, BoundQuery, BoundSelect, DistinctModifier, GroupingSet, LimitModifier,
    OrderByNode,
};
use crate::binder::{Binder, GroupingBindingContext};
use crate::expression::*;
use crate::operator::ColumnBinding;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_parser::ast::{
    Expr as AstExpr, GroupBy, Hint, Literal as AstLiteral, OrderByExpr, SelectStmt, SelectTarget,
    TableReference,
};

impl Binder {
    /// Bind a BoundSelect (the body of a SELECT statement).
    ///
    ///
    /// 1. FROM clause
    /// 2. Expand SELECT list
    /// 3. Record column_count
    /// 4. WHERE clause (with WhereBinder)
    /// 5. PrepareModifiers - ORDER BY (may add to SELECT list)
    /// 6. GROUP BY clause (with GroupBinder)
    /// 7. SELECT list binding
    /// 8. HAVING clause (with HavingBinder)
    /// 9. QUALIFY clause (with QualifyBinder)
    /// 10. BindModifiers - finalize ORDER BY types
    pub fn bind_select_query(
        &mut self,
        select: SelectStmt,
        order_by: &[OrderByExpr],
        limit: &[paro_parser::ast::Expr],
        offset: &Option<paro_parser::ast::Expr>,
    ) -> Result<BoundQuery> {
        let mut aggregates = Vec::new();
        let hnsw_ef_hint = Self::extract_hnsw_ef_hint(select.hints.as_ref())?;
        let projection_index = self.bind_context.generate_table_index();
        let group_index = self.bind_context.generate_table_index();
        let aggregate_index = self.bind_context.generate_table_index();
        let groupings_index = self.bind_context.generate_table_index();
        let window_index = self.bind_context.generate_table_index();
        let prune_index = self.bind_context.generate_table_index();

        // =================================================================
        // 1. Bind FROM clause
        // =================================================================
        let from_table = self.bind_from_clause(&select.from)?;

        // =================================================================
        // 2. Expand SELECT list (star expressions)
        // =================================================================
        let mut expanded_select_list = self.expand_star_expressions(&select.select_list)?;

        // =================================================================
        // 3. Record column_count BEFORE ORDER BY additions
        // =================================================================
        let column_count = expanded_select_list.len();

        // =================================================================
        // 4. Prepare SelectBindState for alias tracking
        // =================================================================
        let mut bind_state = SelectBindState::new();

        // Populate bind_state with aliases and original expressions
        for (i, target) in expanded_select_list.iter().enumerate() {
            if let SelectTarget::AliasedExpr { expr, alias } = target {
                // Record alias mapping
                if let Some(alias_ident) = alias {
                    bind_state.add_alias(&alias_ident.name, alias_ident.quote.is_some(), i);
                }
                // Record original expression (for alias resolution later)
                bind_state.original_expressions.push(*expr.clone());
                // Record both source and qualified forms. ORDER BY may spell
                // the same selected expression with or without a table name.
                bind_state.add_projection(expr.to_string(), i);
                let mut qualified_expr = *expr.clone();
                ExpressionBinder::qualify_column_names(self, &mut qualified_expr);
                bind_state.add_projection(qualified_expr.to_string(), i);
            }
        }

        // =================================================================
        // 5. Bind WHERE clause
        // =================================================================
        let early_alias_lookup = AliasLookup::snapshot(&bind_state);
        let where_clause = self.bind_where_clause_with_binder(
            select.selection,
            early_alias_lookup.clone(),
            &mut bind_state,
        )?;

        // =================================================================
        // 6. PrepareModifiers - Bind ORDER BY (may add expressions to SELECT list)
        //         PrepareModifiers(order_binder, statement, result);
        // =================================================================
        let bound_limit = self.bind_limit(limit, offset)?;
        let order_by_bindings = self.prepare_order_by(
            order_by,
            &mut expanded_select_list,
            early_alias_lookup,
            &mut bind_state,
            !select.distinct,
        )?;

        // =================================================================
        // 7. Bind GROUP BY clause
        // =================================================================
        let (groups, group_info) =
            self.bind_group_by_with_binder(select.group_by, &expanded_select_list, &bind_state)?;

        // =================================================================
        // 8. Bind SELECT list (includes any ORDER BY additions)
        // =================================================================
        let having_ast = select.having;
        let qualify_ast = select.qualify;
        let grouping_context =
            if !groups.group_expressions.is_empty() || !groups.grouping_sets.is_empty() {
                Some(GroupingBindingContext::new(
                    groupings_index,
                    group_info.clone(),
                ))
            } else {
                None
            };
        let (mut select_list, names, types, having_clause, qualify_clause, grouping_functions) =
            self.with_grouping_context(grouping_context, |binder| -> Result<_> {
                let (select_list, names, types) =
                    binder.bind_select_list_with_aliases(expanded_select_list, &mut bind_state)?;
                let late_alias_lookup = AliasLookup::snapshot(&bind_state);
                let having_clause = binder.bind_having_with_aliases(
                    having_ast,
                    late_alias_lookup.clone(),
                    &mut bind_state,
                )?;
                let qualify_clause =
                    binder.bind_qualify_clause(qualify_ast, late_alias_lookup, &mut bind_state)?;
                let grouping_functions = binder
                    .active_grouping_context
                    .as_ref()
                    .map(|ctx| ctx.grouping_functions.clone())
                    .unwrap_or_default();
                Ok((
                    select_list,
                    names,
                    types,
                    having_clause,
                    qualify_clause,
                    grouping_functions,
                ))
            })?;

        // =================================================================
        // 11. BindModifiers - Finalize ORDER BY with types
        // =================================================================
        let bound_order_by = self.finalize_order_by(order_by_bindings, projection_index, &types)?;
        let bound_distinct = Self::bind_distinct(select.distinct);

        // Make bound_order_by mutable for aggregate extraction
        let mut bound_order_by = bound_order_by;

        // =================================================================
        // 12. Extract aggregates from SELECT list, HAVING, and ORDER BY
        // =================================================================
        let group_count = groups.group_expressions.len();
        for expr in &mut select_list {
            let mut e = std::mem::replace(
                expr,
                Expression::Constant(ConstantExpression {
                    value: paro_common::runtime_value::Value::Null(LogicalType::Unknown),
                    return_type: LogicalType::Unknown,
                }),
            );
            e = e.extract_aggregates(&mut aggregates, group_count);
            let replaced = e.replace_groups(&groups.group_expressions);
            *expr = if group_count > 0 || !aggregates.is_empty() {
                Self::replace_aggregate_references_with_column_refs(
                    replaced,
                    group_index,
                    group_count,
                    aggregate_index,
                )
            } else {
                replaced
            };
        }

        let mut bound_having = having_clause;
        if let Some(expr) = bound_having {
            let e = expr.extract_aggregates(&mut aggregates, groups.group_expressions.len());
            let replaced = e.replace_groups(&groups.group_expressions);
            bound_having = Some(if group_count > 0 || !aggregates.is_empty() {
                Self::replace_aggregate_references_with_column_refs(
                    replaced,
                    group_index,
                    group_count,
                    aggregate_index,
                )
            } else {
                replaced
            });
        }

        let mut bound_qualify = qualify_clause;
        if let Some(expr) = bound_qualify {
            let e = expr.extract_aggregates(&mut aggregates, groups.group_expressions.len());
            let replaced = e.replace_groups(&groups.group_expressions);
            bound_qualify = Some(if group_count > 0 || !aggregates.is_empty() {
                Self::replace_aggregate_references_with_column_refs(
                    replaced,
                    group_index,
                    group_count,
                    aggregate_index,
                )
            } else {
                replaced
            });
        }

        if let Some(orders) = &mut bound_order_by {
            for order in orders.iter_mut() {
                let e = std::mem::replace(
                    &mut order.expression,
                    Expression::Constant(ConstantExpression {
                        value: paro_common::runtime_value::Value::Null(LogicalType::Unknown),
                        return_type: LogicalType::Unknown,
                    }),
                );
                let e = e.extract_aggregates(&mut aggregates, groups.group_expressions.len());
                let replaced = e.replace_groups(&groups.group_expressions);
                order.expression = if group_count > 0 || !aggregates.is_empty() {
                    Self::replace_aggregate_references_with_column_refs(
                        replaced,
                        group_index,
                        group_count,
                        aggregate_index,
                    )
                } else {
                    replaced
                };
            }

            // Simplify ORDER BY (remove duplicates and constants)
            Self::simplify_order_by(orders);
        }

        // =================================================================
        // 13. Determine need_prune
        // This is set when ORDER BY adds extra columns to the SELECT list
        // =================================================================
        let need_prune = select_list.len() > column_count;

        // =================================================================
        // 14. Truncate names and types to only original columns
        // ORDER BY additions are in select_list but not in names/types
        // =================================================================
        let names: Vec<String> = names.into_iter().take(column_count).collect();
        let types: Vec<LogicalType> = types.into_iter().take(column_count).collect();

        // =================================================================
        // 15. Create BoundSelect
        // =================================================================
        let bound_node = BoundSelect {
            from_table,
            select_list,
            names,
            types,
            projection_index,
            where_clause,
            distinct: bound_distinct,
            groups,
            aggregates,
            having_clause: bound_having,
            group_index,
            aggregate_index,
            grouping_functions,
            groupings_index,
            window_index,
            qualify_clause: bound_qualify,
            windows: Vec::new(), // Window functions are extracted once their child plan is known.
        };

        Ok(BoundQuery::Select(Box::new(bound_node)).with_modifiers(
            bound_order_by,
            bound_limit,
            hnsw_ef_hint,
            need_prune.then_some(prune_index),
        ))
    }

    fn extract_hnsw_ef_hint(hints: Option<&Hint>) -> Result<Option<usize>> {
        let Some(hints) = hints else {
            return Ok(None);
        };

        let mut ef = None;
        for hint in &hints.hints_list {
            if !hint.name.name.eq_ignore_ascii_case("hnsw_ef") {
                continue;
            }
            let value = Self::parse_positive_hint_usize(&hint.expr, "HNSW_EF")?;
            ef = Some(value);
        }
        Ok(ef)
    }

    fn parse_positive_hint_usize(expr: &AstExpr, hint_name: &str) -> Result<usize> {
        let raw = match expr {
            AstExpr::Literal {
                value: AstLiteral::UInt64(v),
                ..
            } => *v,
            _ => {
                return Err(paro_error::invalid_input(format!(
                    "{} hint expects a positive integer literal, e.g. /*+ {}(256) */",
                    hint_name, hint_name
                )));
            }
        };
        if raw == 0 {
            return Err(paro_error::invalid_input(format!(
                "{} hint must be greater than 0",
                hint_name
            )));
        }
        usize::try_from(raw).map_err(|_| {
            paro_error::invalid_input(format!(
                "{} hint is too large for platform usize: {}",
                hint_name, raw
            ))
        })
    }

    /// Replace aggregate/group `BoundReference` nodes with `BoundColumnRef` nodes
    /// bound to the aggregate operator output.
    ///
    /// in this logical phase.
    fn replace_aggregate_references_with_column_refs(
        mut expr: Expression,
        group_index: usize,
        group_count: usize,
        aggregate_index: usize,
    ) -> Expression {
        Self::replace_aggregate_references_in_place(
            &mut expr,
            group_index,
            group_count,
            aggregate_index,
        );
        expr
    }

    fn replace_aggregate_references_in_place(
        expr: &mut Expression,
        group_index: usize,
        group_count: usize,
        aggregate_index: usize,
    ) {
        if let Expression::Reference(reference) = expr {
            let (table_index, column_index) = if reference.index < group_count {
                (group_index, reference.index)
            } else {
                (aggregate_index, reference.index - group_count)
            };
            *expr = Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(table_index, column_index),
                reference.return_type.clone(),
            ));
            return;
        }

        ExpressionIterator::enumerate_children_mut(expr, |child| {
            Self::replace_aggregate_references_in_place(
                child,
                group_index,
                group_count,
                aggregate_index,
            );
        });
    }

    /// Expand star expressions in SELECT list.
    ///
    fn expand_star_expressions(
        &mut self,
        select_list: &[SelectTarget],
    ) -> Result<Vec<SelectTarget>> {
        let mut expanded = Vec::new();

        for item in select_list {
            match item {
                SelectTarget::StarColumns { qualified, .. } => {
                    // Extract table name from qualified path
                    let table_name = if qualified.len() == 1 {
                        match &qualified[0] {
                            paro_parser::ast::Indirection::Star(_) => None,
                            paro_parser::ast::Indirection::Identifier(ident) => {
                                Some(ident.name.clone())
                            }
                        }
                    } else if qualified.len() == 2 {
                        match &qualified[0] {
                            paro_parser::ast::Indirection::Identifier(ident) => {
                                Some(ident.name.clone())
                            }
                            _ => None,
                        }
                    } else {
                        None
                    };

                    // Generate column expressions for the star
                    let exprs = self
                        .bind_context
                        .generate_all_column_expressions(table_name.as_deref());

                    if exprs.is_empty() {
                        return Err(paro_error::syntax("SELECT * with no tables in FROM clause"));
                    }

                    // Convert to AliasedExpr targets
                    for expr in exprs {
                        expanded.push(SelectTarget::AliasedExpr {
                            expr: Box::new(expr.into()),
                            alias: None,
                        });
                    }
                }
                SelectTarget::AliasedExpr { .. } => {
                    expanded.push(item.clone());
                }
            }
        }

        if expanded.is_empty() {
            return Err(paro_error::syntax(
                "SELECT list is empty after resolving * expressions",
            ));
        }

        Ok(expanded)
    }

    /// Bind WHERE clause using WhereBinder.
    ///
    fn bind_where_clause_with_binder(
        &mut self,
        selection: Option<paro_parser::ast::Expr>,
        alias_lookup: AliasLookup,
        bind_state: &mut SelectBindState,
    ) -> Result<Option<Expression>> {
        if let Some(selection) = selection {
            let mut where_binder = WhereBinder::with_alias_lookup(self, alias_lookup, bind_state);

            let bound = where_binder.bind(selection)?;
            Ok(Some(bound))
        } else {
            Ok(None)
        }
    }

    /// Bind GROUP BY clause using GroupBinder.
    ///
    fn bind_group_by_with_binder(
        &mut self,
        group_by: Option<GroupBy>,
        select_list: &[SelectTarget],
        bind_state: &SelectBindState,
    ) -> Result<(crate::binder::ir::Groups, BoundGroupInformation)> {
        match group_by {
            None => Ok((
                crate::binder::ir::Groups::default(),
                BoundGroupInformation::new(),
            )),
            Some(GroupBy::Normal(exprs)) => {
                let mut bound_group_exprs = Vec::new();
                let mut group_info = BoundGroupInformation::new();

                // Create GroupBinder
                let mut group_binder = GroupBinder::new(self, select_list, bind_state);

                // Bind each GROUP BY expression
                for (i, expr) in exprs.into_iter().enumerate() {
                    group_binder.set_bind_index(i);
                    let mut normalized_expr = expr.clone();
                    ExpressionBinder::qualify_column_names(
                        group_binder.base.binder,
                        &mut normalized_expr,
                    );
                    let group_idx = bound_group_exprs.len();
                    bound_group_exprs.push(group_binder.bind(expr)?);
                    group_info.add_group(normalized_expr.to_string(), group_idx);
                    if let AstExpr::ColumnRef { column, .. } = &normalized_expr {
                        if column.schema.is_none() && column.table.is_none() {
                            group_info.add_alias(column.column.name(), group_idx);
                        }
                    }
                }

                Ok((
                    crate::binder::ir::Groups {
                        group_expressions: bound_group_exprs,
                        grouping_sets: Vec::new(),
                    },
                    group_info,
                ))
            }
            Some(GroupBy::All) => Err(paro_error::not_implemented("GROUP BY ALL")),
            Some(group_by) => {
                self.bind_advanced_group_by_with_binder(group_by, select_list, bind_state)
            }
        }
    }

    fn bind_advanced_group_by_with_binder(
        &mut self,
        group_by: GroupBy,
        select_list: &[SelectTarget],
        bind_state: &SelectBindState,
    ) -> Result<(crate::binder::ir::Groups, BoundGroupInformation)> {
        let grouping_set_exprs = Self::expand_group_by_sets(group_by)?;
        let mut group_binder = GroupBinder::new(self, select_list, bind_state);
        let mut bound_group_exprs = Vec::new();
        let mut grouping_sets = Vec::with_capacity(grouping_set_exprs.len());
        let mut group_info = BoundGroupInformation::new();

        for (set_idx, set_exprs) in grouping_set_exprs.into_iter().enumerate() {
            let mut bound_set = Vec::new();
            for expr in set_exprs {
                group_binder.set_bind_index(set_idx);
                let mut normalized_expr = expr.clone();
                ExpressionBinder::qualify_column_names(
                    group_binder.base.binder,
                    &mut normalized_expr,
                );
                let bound_expr = group_binder.bind(expr)?;
                let group_idx =
                    Self::insert_or_get_group_expression(&mut bound_group_exprs, bound_expr);
                if !bound_set.contains(&group_idx) {
                    bound_set.push(group_idx);
                }
                group_info.add_group(normalized_expr.to_string(), group_idx);
                if let AstExpr::ColumnRef { column, .. } = &normalized_expr {
                    if column.schema.is_none() && column.table.is_none() {
                        group_info.add_alias(column.column.name(), group_idx);
                    }
                }
            }
            grouping_sets.push(GroupingSet {
                expressions: bound_set,
            });
        }

        Ok((
            crate::binder::ir::Groups {
                group_expressions: bound_group_exprs,
                grouping_sets,
            },
            group_info,
        ))
    }

    fn insert_or_get_group_expression(
        group_exprs: &mut Vec<Expression>,
        bound_expr: Expression,
    ) -> usize {
        if bound_expr.evaluation_properties().can_share_evaluation() {
            if let Some(existing_idx) = group_exprs
                .iter()
                .position(|existing| existing.equals(&bound_expr))
            {
                return existing_idx;
            }
        }
        let new_idx = group_exprs.len();
        group_exprs.push(bound_expr);
        new_idx
    }

    fn expand_group_by_sets(group_by: GroupBy) -> Result<Vec<Vec<AstExpr>>> {
        match group_by {
            GroupBy::GroupingSets(sets) => Ok(sets),
            GroupBy::Cube(exprs) => Self::expand_cube(exprs),
            GroupBy::Rollup(exprs) => Ok(Self::expand_rollup(exprs)),
            GroupBy::Combined(items) => {
                let mut combined_sets = vec![Vec::new()];
                for item in items {
                    let item_sets = match item {
                        GroupBy::Normal(exprs) => vec![exprs],
                        GroupBy::GroupingSets(sets) => sets,
                        GroupBy::Cube(exprs) => Self::expand_cube(exprs)?,
                        GroupBy::Rollup(exprs) => Self::expand_rollup(exprs),
                        GroupBy::Combined(nested) => {
                            Self::expand_group_by_sets(GroupBy::Combined(nested))?
                        }
                        GroupBy::All => {
                            return Err(paro_error::not_implemented(
                                "GROUP BY ALL inside combined GROUP BY",
                            ));
                        }
                    };

                    let mut next_sets = Vec::new();
                    for base in &combined_sets {
                        for set in &item_sets {
                            let mut merged = base.clone();
                            merged.extend(set.clone());
                            next_sets.push(merged);
                        }
                    }
                    combined_sets = next_sets;
                }
                Ok(combined_sets)
            }
            GroupBy::Normal(exprs) => Ok(vec![exprs]),
            GroupBy::All => Err(paro_error::not_implemented("GROUP BY ALL")),
        }
    }

    fn expand_rollup(exprs: Vec<AstExpr>) -> Vec<Vec<AstExpr>> {
        let mut sets = Vec::with_capacity(exprs.len() + 1);
        for prefix_len in (0..=exprs.len()).rev() {
            sets.push(exprs[..prefix_len].to_vec());
        }
        sets
    }

    fn expand_cube(exprs: Vec<AstExpr>) -> Result<Vec<Vec<AstExpr>>> {
        if exprs.len() >= usize::BITS as usize {
            return Err(paro_error::not_implemented(format!(
                "CUBE with {} expressions is not supported",
                exprs.len()
            )));
        }

        let subset_count = 1usize << exprs.len();
        let mut sets = Vec::with_capacity(subset_count);
        for mask in (0..subset_count).rev() {
            let mut set = Vec::new();
            for (idx, expr) in exprs.iter().enumerate() {
                if (mask & (1usize << idx)) != 0 {
                    set.push(expr.clone());
                }
            }
            sets.push(set);
        }
        Ok(sets)
    }

    /// Bind DISTINCT clause.
    fn bind_distinct(distinct: bool) -> Option<DistinctModifier> {
        distinct.then(DistinctModifier::distinct)
    }

    /// Bind GROUP BY clause (legacy - delegates to bind_group_by_with_binder).
    pub fn bind_group_by(
        &mut self,
        group_by: Option<GroupBy>,
        select_list: &[SelectTarget],
    ) -> Result<crate::binder::ir::Groups> {
        // Create a temporary SelectBindState
        let mut bind_state = SelectBindState::default();
        for (i, target) in select_list.iter().enumerate() {
            if let SelectTarget::AliasedExpr { expr, alias } = target {
                if let Some(alias_ident) = alias {
                    bind_state.add_alias(&alias_ident.name, alias_ident.quote.is_some(), i);
                }
                bind_state.original_expressions.push(*expr.clone());
            }
        }

        self.bind_group_by_with_binder(group_by, select_list, &bind_state)
            .map(|(groups, _)| groups)
    }

    /// Bind HAVING clause with alias resolution.
    pub fn bind_having_with_aliases(
        &mut self,
        having: Option<paro_parser::ast::Expr>,
        alias_lookup: AliasLookup,
        bind_state: &mut SelectBindState,
    ) -> Result<Option<Expression>> {
        if let Some(expr) = having {
            let mut having_binder = HavingBinder::new(self, bind_state, alias_lookup);
            Ok(Some(having_binder.bind(expr)?))
        } else {
            Ok(None)
        }
    }

    /// Bind QUALIFY clause using QualifyBinder.
    ///
    /// QUALIFY filters window function results (like HAVING for aggregates).
    fn bind_qualify_clause(
        &mut self,
        qualify: Option<paro_parser::ast::Expr>,
        alias_lookup: AliasLookup,
        bind_state: &mut SelectBindState,
    ) -> Result<Option<Expression>> {
        if let Some(expr) = qualify {
            let mut qualify_binder = QualifyBinder::new(self, bind_state, alias_lookup);
            let bound = qualify_binder.bind(expr)?;
            Ok(Some(bound))
        } else {
            Ok(None)
        }
    }

    pub fn bind_from_clause(&mut self, from: &[TableReference]) -> Result<Option<BoundFromItem>> {
        let Some((first, rest)) = from.split_first() else {
            return Ok(None);
        };

        let mut bound = self.bind_table_ref(first.clone())?;
        for table_ref in rest {
            bound = crate::binder::bind::from::join::bind_implicit_cross_join(
                self,
                bound,
                table_ref.clone(),
            )?;
        }

        Ok(Some(bound))
    }

    /// Bind SELECT list and record expression aliases
    pub fn bind_select_list_with_aliases(
        &mut self,
        select_list: Vec<SelectTarget>,
        bind_state: &mut SelectBindState,
    ) -> Result<(Vec<Expression>, Vec<String>, Vec<LogicalType>)> {
        let mut result_list = Vec::new();
        let mut names = Vec::new();
        let mut types = Vec::new();
        let mut index = 0;

        for item in select_list {
            match item {
                SelectTarget::StarColumns { qualified, .. } => {
                    let table_name = if qualified.len() == 1 {
                        match &qualified[0] {
                            paro_parser::ast::Indirection::Star(_) => None,
                            paro_parser::ast::Indirection::Identifier(ident) => {
                                Some(ident.name.clone())
                            }
                        }
                    } else if qualified.len() == 2 {
                        match &qualified[0] {
                            paro_parser::ast::Indirection::Identifier(ident) => {
                                Some(ident.name.clone())
                            }
                            _ => None,
                        }
                    } else {
                        None
                    };

                    let exprs = self
                        .bind_context
                        .generate_all_column_expressions(table_name.as_deref());
                    if exprs.is_empty() {
                        return Err(paro_error::syntax("SELECT * with no tables in FROM clause"));
                    }
                    for expr in exprs {
                        let name = expr.to_string();
                        let bound = expr::bind_expression(self, expr)?;
                        names.push(name);
                        types.push(bound.return_type());
                        result_list.push(bound);
                        index += 1;
                    }
                }
                SelectTarget::AliasedExpr { expr, alias } => {
                    let bound = {
                        let mut select_binder = SelectBinder::new(self, bind_state);
                        select_binder.bind_at_index((*expr).clone(), index)?
                    };
                    if let Some(alias_ident) = alias {
                        names.push(alias_ident.name);
                    } else {
                        let name = expr.to_string();
                        names.push(name);
                    }
                    types.push(bound.return_type());
                    result_list.push(bound);
                    index += 1;
                }
            }
        }
        Ok((result_list, names, types))
    }

    pub fn bind_where_clause(
        &mut self,
        selection: Option<paro_parser::ast::Expr>,
    ) -> Result<Option<Expression>> {
        if let Some(selection) = selection {
            let bound = expr::bind_expression(self, selection)?;
            Ok(Some(bound))
        } else {
            Ok(None)
        }
    }

    pub fn bind_limit(
        &mut self,
        limit: &[paro_parser::ast::Expr],
        offset: &Option<paro_parser::ast::Expr>,
    ) -> Result<Option<LimitModifier>> {
        if limit.is_empty() && offset.is_none() {
            return Ok(None);
        }

        let limit_expr = if !limit.is_empty() {
            Some(expr::bind_expression(self, limit[0].clone())?)
        } else {
            None
        };

        let offset_expr = if let Some(off) = offset {
            Some(expr::bind_expression(self, off.clone())?)
        } else {
            None
        };

        Ok(Some(LimitModifier {
            limit: limit_expr,
            offset: offset_expr,
        }))
    }

    /// Simplify ORDER BY clause
    ///
    pub fn simplify_order_by(orders: &mut Vec<OrderByNode>) {
        if orders.is_empty() {
            return;
        }
        let mut seen_expressions = Vec::new();
        let mut new_orders = Vec::new();
        for order in orders.drain(..) {
            if matches!(order.expression, Expression::Constant(_)) {
                continue;
            }
            if !order
                .expression
                .evaluation_properties()
                .can_share_evaluation()
            {
                new_orders.push(order);
                continue;
            }
            let mut is_duplicate = false;
            for existing_expr in &seen_expressions {
                if order.expression.equals(existing_expr) {
                    is_duplicate = true;
                    break;
                }
            }
            if !is_duplicate {
                seen_expressions.push(order.expression.clone());
                new_orders.push(order);
            }
        }
        *orders = new_orders;
    }
    ///
    /// This phase binds ORDER BY expressions and may add them to the SELECT list.
    /// Returns OrderByBinding objects that will be finalized after SELECT list binding.
    fn prepare_order_by(
        &mut self,
        order_by: &[OrderByExpr],
        select_list: &mut Vec<SelectTarget>,
        alias_lookup: AliasLookup,
        bind_state: &mut SelectBindState,
        allow_extra_expressions: bool,
    ) -> Result<Option<Vec<(OrderByBinding, Option<bool>, Option<bool>)>>> {
        if order_by.is_empty() {
            return Ok(None);
        }

        let mut order_bindings = Vec::new();
        if allow_extra_expressions {
            let mut order_binder =
                OrderBinder::with_extra_list(self, bind_state, alias_lookup, select_list);
            for order in order_by {
                let order_binding = order_binder.bind(order.expr.clone())?;
                order_bindings.push((order_binding, order.asc, order.nulls_first));
            }
        } else {
            let mut order_binder = OrderBinder::new(self, bind_state, alias_lookup);
            order_binder.set_query_component("SELECT DISTINCT ORDER BY".to_string());
            for order in order_by {
                let order_binding = order_binder.bind(order.expr.clone())?;
                order_bindings.push((order_binding, order.asc, order.nulls_first));
            }
        }

        Ok(Some(order_bindings))
    }
    ///
    /// This phase converts OrderByBinding to Expression with proper types.
    fn finalize_order_by(
        &self,
        order_bindings: Option<Vec<(OrderByBinding, Option<bool>, Option<bool>)>>,
        projection_index: usize,
        select_list_types: &[LogicalType],
    ) -> Result<Option<Vec<OrderByNode>>> {
        let bindings = match order_bindings {
            Some(b) => b,
            None => return Ok(None),
        };

        let mut orders = Vec::new();

        for (order_binding, asc, nulls_first) in bindings {
            // Get the type for this index from the bound SELECT list
            let return_type = select_list_types
                .get(order_binding.index)
                .cloned()
                .ok_or_else(|| {
                    paro_error::syntax("ORDER BY position is not in the query result")
                })?;

            let bound_expr =
                order_binding.to_bound_expression_with_type(projection_index, return_type);

            let ascending = asc.unwrap_or(true);
            let nulls_first = nulls_first.unwrap_or(!ascending);
            orders.push(OrderByNode {
                expression: bound_expr,
                ascending,
                nulls_first,
            });
        }

        Ok(Some(orders))
    }
}

#[cfg(test)]
mod tests {
    use super::Binder;
    use crate::binder::ir::OrderByNode;
    use crate::expression::{
        ColumnRefExpression, Expression, FunctionExpression, WindowExpression, WindowFrame,
    };
    use crate::operator::ColumnBinding;
    use paro_common::types::LogicalType;
    use paro_function::window::WindowFunction;

    fn row_number(partition_column: usize) -> Expression {
        Expression::Window(WindowExpression::native(
            WindowFunction::row_number(),
            vec![],
            vec![Expression::ColumnRef(ColumnRefExpression::new(
                ColumnBinding::new(10, partition_column),
                LogicalType::Integer,
            ))],
            vec![],
            WindowFrame::default(),
            false,
        ))
    }

    fn random_call() -> Expression {
        let function = paro_function::scalar::math::get_random_function()
            .functions
            .into_iter()
            .next()
            .expect("random overload");
        Expression::Function(FunctionExpression::new(
            function,
            vec![],
            LogicalType::Double,
        ))
    }

    #[test]
    fn simplify_order_by_preserves_windows_with_different_partitions() {
        let mut orders = vec![
            OrderByNode {
                expression: row_number(0),
                ascending: true,
                nulls_first: false,
            },
            OrderByNode {
                expression: row_number(1),
                ascending: true,
                nulls_first: false,
            },
        ];

        Binder::simplify_order_by(&mut orders);

        assert_eq!(orders.len(), 2);
    }

    #[test]
    fn simplify_order_by_preserves_duplicate_volatile_expressions() {
        let random = random_call();
        let mut orders = vec![
            OrderByNode {
                expression: random.clone(),
                ascending: true,
                nulls_first: false,
            },
            OrderByNode {
                expression: random,
                ascending: true,
                nulls_first: false,
            },
        ];

        Binder::simplify_order_by(&mut orders);

        assert_eq!(orders.len(), 2);
    }

    #[test]
    fn group_expression_dedup_preserves_volatile_calls() {
        let random = random_call();
        let mut groups = Vec::new();

        let first = Binder::insert_or_get_group_expression(&mut groups, random.clone());
        let second = Binder::insert_or_get_group_expression(&mut groups, random);

        assert_eq!((first, second), (0, 1));
        assert_eq!(groups.len(), 2);
    }

    #[test]
    fn aggregate_reference_replacement_visits_window_frame_offsets() {
        let mut window = row_number(0);
        let Expression::Window(expression) = &mut window else {
            unreachable!();
        };
        expression.frame.start_bound =
            crate::expression::WindowFrameBound::Offset(Box::new(Expression::Reference(
                crate::expression::ReferenceExpression::new(1, LogicalType::Integer),
            )));

        let replaced = Binder::replace_aggregate_references_with_column_refs(window, 20, 1, 30);

        let Expression::Window(expression) = replaced else {
            panic!("expected window expression");
        };
        let crate::expression::WindowFrameBound::Offset(offset) = expression.frame.start_bound
        else {
            panic!("expected window frame offset");
        };
        let Expression::ColumnRef(column) = *offset else {
            panic!("expected column reference");
        };
        assert_eq!(column.binding, ColumnBinding::new(30, 0));
    }
}
