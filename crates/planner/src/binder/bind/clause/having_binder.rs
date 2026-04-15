use std::collections::HashSet;

use crate::binder::bind::expr::ExpressionBinder;
use crate::binder::ir::BoundSelect;
use crate::expression::Expression;
use crate::operator::ColumnBinding;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_parser::ast::{ColumnRef, Expr};

use super::{AliasLookup, BaseSelectBinder, BoundGroupInformation, SelectBindState};

/// Aggregate handling mode for HAVING binder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateHandling {
    /// Standard aggregate handling - columns must be in GROUP BY or aggregates.
    StandardHandling,
    /// Force aggregates - used for GROUP BY ALL.
    ForceAggregates,
}

impl Default for AggregateHandling {
    fn default() -> Self {
        Self::StandardHandling
    }
}

/// Binder for HAVING clause expressions.
///
pub struct HavingBinder<'a> {
    pub base: BaseSelectBinder<'a>,
    alias_lookup: Option<AliasLookup>,
    visited_select_indexes: HashSet<usize>,
    aggregate_handling: AggregateHandling,
}

impl<'a> HavingBinder<'a> {
    /// Create a new HavingBinder.
    pub fn new(
        binder: &'a mut crate::binder::Binder,
        bind_state: &'a mut SelectBindState,
        alias_lookup: AliasLookup,
    ) -> Self {
        let mut res = Self {
            base: BaseSelectBinder::new(binder, bind_state),
            alias_lookup: Some(alias_lookup),
            visited_select_indexes: HashSet::new(),
            aggregate_handling: AggregateHandling::default(),
        };
        res.base.base.target_type = LogicalType::Boolean;
        res.base.base.allow_window = false;
        res
    }

    /// Create a new HavingBinder with node and group info.
    pub fn with_node_and_info(
        binder: &'a mut crate::binder::Binder,
        bind_state: &'a mut SelectBindState,
        node: &'a mut BoundSelect,
        info: &'a BoundGroupInformation,
        alias_lookup: AliasLookup,
        aggregate_handling: AggregateHandling,
    ) -> Self {
        let mut res = Self {
            base: BaseSelectBinder::with_node_and_info(binder, bind_state, node, info),
            alias_lookup: Some(alias_lookup),
            visited_select_indexes: HashSet::new(),
            aggregate_handling,
        };
        res.base.base.target_type = LogicalType::Boolean;
        res.base.base.allow_window = false;
        res
    }

    /// Bind the HAVING clause.
    pub fn bind(&mut self, expr: Expr) -> Result<Expression> {
        self.bind_expression(expr)
    }

    /// Bind an expression, handling column references and aliases.
    pub fn bind_expression(&mut self, expr: Expr) -> Result<Expression> {
        match expr {
            Expr::ColumnRef { ref column, .. } => self.bind_column_ref(column),
            _ => self.base.bind_expression(expr),
        }
    }

    /// Bind a column reference.
    ///
    pub fn bind_column_ref(&mut self, colref: &ColumnRef) -> Result<Expression> {
        // 1. Check if it's a lambda reference (not implemented in Paro yet)

        // 2. Check if it's a SQL value function
        if !colref.is_qualified() {
            if let Some(value_func) =
                ExpressionBinder::get_sql_value_function(&self.base.base, colref.column.name())
            {
                return self.bind_expression(value_func);
            }
        }

        if ExpressionBinder::is_potential_alias(colref) {
            if let Some(alias_lookup) = self.alias_lookup.as_ref() {
                let alias_name = colref.column.name().to_lowercase();
                if let Some((index, original_expr)) = alias_lookup.resolve_alias(&alias_name)? {
                    if !self.visited_select_indexes.contains(&index) {
                        self.base.bind_state.mark_alias_referenced(index);
                        self.visited_select_indexes.insert(index);
                        let result = self.bind_expression(original_expr);
                        self.visited_select_indexes.remove(&index);
                        return result;
                    }
                }
            }
        }

        // 4. Try regular binding (handles GROUP BY)
        let result = self.base.bind_column_ref(colref.clone());
        if result.is_ok() {
            return result;
        }

        // 5. Aggregate handling
        if self.aggregate_handling != AggregateHandling::ForceAggregates {
            return Err(paro_error::syntax(format!(
                "column \"{}\" must appear in the GROUP BY clause or be used in an aggregate function",
                colref.column.name()
            )));
        }

        // 6. FORCE_AGGREGATES mode: push to GROUP BY
        let bound = self.base.base.bind_expression(Expr::ColumnRef {
            span: colref.column.span(),
            column: colref.clone(),
        })?;

        let node = self.base.node.as_mut().unwrap();
        let group_index = node.groups.group_expressions.len();
        let return_type = bound.return_type();
        let binding = ColumnBinding::new(node.group_index, group_index);

        node.groups.group_expressions.push(bound);

        Ok(Expression::ColumnRef(
            crate::expression::ColumnRefExpression {
                return_type,
                binding,
                depth: 0,
            },
        ))
    }
}
