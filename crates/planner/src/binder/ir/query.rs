// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Binder-owned query IR.

use crate::expression::CastExpression;
use crate::expression::Expression;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_function::scalar::cast::CastFunctionSet;

use super::cte::WithCTE;
use super::from::BoundFromItem;

#[derive(Debug, Clone)]
pub enum BoundQuery {
    With(Box<WithCTE>),
    Select(Box<BoundSelect>),
    Values(BoundValues),
    SetOperation(Box<BoundSetOperation>),
}

impl BoundQuery {
    pub fn names(&self) -> Vec<String> {
        match self {
            BoundQuery::With(n) => n.child.names(),
            BoundQuery::Select(n) => n.names.clone(),
            BoundQuery::Values(n) => n.names.clone(),
            BoundQuery::SetOperation(n) => n.names.clone(),
        }
    }

    pub fn types(&self) -> Vec<LogicalType> {
        match self {
            BoundQuery::With(n) => n.child.types(),
            BoundQuery::Select(n) => n.types.clone(),
            BoundQuery::Values(n) => n.types.clone(),
            BoundQuery::SetOperation(n) => n.types.clone(),
        }
    }

    pub fn cast_to_types(
        &mut self,
        target_types: &[LogicalType],
        cast_functions: &CastFunctionSet,
    ) -> Result<()> {
        let current_types = self.types();
        if current_types.len() != target_types.len() {
            return Err(paro_error::catalog(format!(
                "BoundQuery::cast_to_types: column count mismatch (expected {}, found {})",
                target_types.len(),
                current_types.len()
            )));
        }

        if current_types == target_types {
            return Ok(());
        }

        match self {
            BoundQuery::With(n) => {
                n.child.cast_to_types(target_types, cast_functions)?;
            }
            BoundQuery::Select(n) => {
                for (i, target_type) in target_types.iter().enumerate() {
                    if n.types[i] != *target_type {
                        n.select_list[i] = CastExpression::add_explicit_cast(
                            n.select_list[i].clone(),
                            target_type.clone(),
                            cast_functions,
                            false,
                        )?;
                        n.types[i] = target_type.clone();
                    }
                }
            }
            BoundQuery::Values(n) => {
                n.cast_rows_to_types(target_types, cast_functions)?;
            }
            BoundQuery::SetOperation(n) => {
                n.left.cast_to_types(target_types, cast_functions)?;
                n.right.cast_to_types(target_types, cast_functions)?;
                n.types = target_types.to_vec();
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum DistinctType {
    #[default]
    Distinct,
    DistinctOn,
}

#[derive(Debug, Clone)]
pub struct DistinctModifier {
    pub distinct_type: DistinctType,
    pub target_distincts: Vec<Expression>,
}

impl DistinctModifier {
    pub fn distinct() -> Self {
        Self {
            distinct_type: DistinctType::Distinct,
            target_distincts: Vec::new(),
        }
    }

    pub fn distinct_on(targets: Vec<Expression>) -> Self {
        Self {
            distinct_type: DistinctType::DistinctOn,
            target_distincts: targets,
        }
    }

    pub fn is_distinct_on(&self) -> bool {
        self.distinct_type == DistinctType::DistinctOn
    }
}

#[derive(Debug, Clone, Default)]
pub struct GroupingSet {
    pub expressions: Vec<usize>,
}

#[derive(Debug, Clone, Default)]
pub struct Groups {
    pub group_expressions: Vec<Expression>,
    pub grouping_sets: Vec<GroupingSet>,
}

#[derive(Debug, Clone)]
pub struct BoundSelect {
    pub from_table: Option<BoundFromItem>,
    pub select_list: Vec<Expression>,
    pub names: Vec<String>,
    pub types: Vec<LogicalType>,
    pub projection_index: usize,
    pub where_clause: Option<Expression>,
    pub distinct: Option<DistinctModifier>,
    pub limit: Option<LimitModifier>,
    pub order_by: Option<Vec<OrderByNode>>,
    pub hnsw_ef_hint: Option<usize>,
    pub groups: Groups,
    pub aggregates: Vec<Expression>,
    pub having_clause: Option<Expression>,
    pub group_index: usize,
    pub aggregate_index: usize,
    pub grouping_functions: Vec<Vec<usize>>,
    pub groupings_index: usize,
    pub window_index: usize,
    pub prune_index: usize,
    pub column_count: usize,
    pub need_prune: bool,
    pub qualify_clause: Option<Expression>,
    pub windows: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct LimitModifier {
    pub limit: Option<Expression>,
    pub offset: Option<Expression>,
}

#[derive(Debug, Clone)]
pub struct OrderByNode {
    pub expression: Expression,
    pub ascending: bool,
    pub nulls_first: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOperationType {
    Union,
    UnionAll,
    Intersect,
    IntersectAll,
    Except,
    ExceptAll,
}

#[derive(Debug, Clone)]
pub struct BoundSetOperation {
    pub table_index: usize,
    pub setop_type: SetOperationType,
    pub left: Box<BoundQuery>,
    pub right: Box<BoundQuery>,
    pub names: Vec<String>,
    pub types: Vec<LogicalType>,
}

#[derive(Debug, Clone)]
pub struct BoundValues {
    pub projection_index: usize,
    pub values: Vec<Vec<Expression>>,
    pub names: Vec<String>,
    pub types: Vec<LogicalType>,
}

impl BoundValues {
    /// Align every row expression with the declared column types.
    ///
    /// `types` describes the common type of each VALUES column, so leaving a
    /// narrower expression in an individual row would make the logical schema
    /// disagree with the expression matrix consumed by execution.
    pub(crate) fn cast_rows_to_types(
        &mut self,
        target_types: &[LogicalType],
        cast_functions: &CastFunctionSet,
    ) -> Result<()> {
        for (row_idx, row) in self.values.iter_mut().enumerate() {
            if row.len() != target_types.len() {
                return Err(paro_error::internal(format!(
                    "VALUES row {row_idx} has {} expressions but schema has {} columns",
                    row.len(),
                    target_types.len()
                )));
            }
            for (expression, target_type) in row.iter_mut().zip(target_types) {
                if expression.return_type() != *target_type {
                    *expression = CastExpression::add_explicit_cast(
                        expression.clone(),
                        target_type.clone(),
                        cast_functions,
                        false,
                    )?;
                }
            }
        }
        self.types = target_types.to_vec();
        Ok(())
    }
}
