//! Binder-owned FROM clause IR.

use std::sync::Arc;

use crate::binder::bind::graph::BoundPatternElement;
use crate::binder::CorrelatedColumnInfo;
use crate::expression::Expression;
use paro_catalog::entry::{PropertyGraphCatalogEntry, TableCatalogEntry};
use paro_common::types::LogicalType;
use paro_parser::ast::PathMode;

use super::query::BoundQuery;

#[derive(Debug, Clone)]
pub enum BoundFromItem {
    BaseTable(BoundBaseTable),
    Join(BoundJoin),
    Subquery(BoundFromSubquery),
    TableFunction(BoundTableFunction),
    CTE(BoundFromCTE),
    GraphTable(BoundFromGraphTable),
}

#[derive(Debug, Clone)]
pub struct BoundBaseTable {
    pub table: Arc<TableCatalogEntry>,
    pub table_index: usize,
    pub relation_name: String,
    pub relation_alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BoundFromCTE {
    pub cte_index: usize,
    pub alias: String,
    pub column_names: Vec<String>,
    pub column_types: Vec<LogicalType>,
    pub table_index: usize,
}

#[derive(Debug, Clone)]
pub struct BoundFromGraphTable {
    pub graph_entry: Arc<PropertyGraphCatalogEntry>,
    pub bound_pattern: BoundGraphPattern,
    pub bound_columns: Vec<BoundGraphColumn>,
    pub table_index: usize,
    pub output_names: Vec<String>,
    pub output_types: Vec<LogicalType>,
    pub path_mode: Option<PathMode>,
    pub has_path_functions: bool,
    pub path_length_col_idx: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct BoundGraphPattern {
    pub elements: Vec<BoundPatternElement>,
}

#[derive(Debug, Clone)]
pub struct BoundGraphColumn {
    pub expr: Expression,
    pub alias: String,
    pub logical_type: LogicalType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
}

impl std::fmt::Display for JoinType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinType::Inner => write!(f, "INNER"),
            JoinType::Left => write!(f, "LEFT"),
            JoinType::Right => write!(f, "RIGHT"),
            JoinType::Full => write!(f, "FULL"),
            JoinType::Cross => write!(f, "CROSS"),
            JoinType::LeftSemi => write!(f, "LEFT SEMI"),
            JoinType::RightSemi => write!(f, "RIGHT SEMI"),
            JoinType::LeftAnti => write!(f, "LEFT ANTI"),
            JoinType::RightAnti => write!(f, "RIGHT ANTI"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BoundJoin {
    pub left: Box<BoundFromItem>,
    pub right: Box<BoundFromItem>,
    pub condition: Option<Expression>,
    pub join_type: JoinType,
    pub lateral: bool,
    pub correlated_columns: Vec<CorrelatedColumnInfo>,
}

#[derive(Debug, Clone)]
pub struct BoundFromSubquery {
    pub subquery: Box<BoundQuery>,
    pub alias: String,
    pub column_names: Vec<String>,
    pub column_types: Vec<LogicalType>,
    pub subquery_index: usize,
    pub lateral: bool,
    pub correlated_columns: Vec<CorrelatedColumnInfo>,
}

#[derive(Debug, Clone)]
pub struct BoundTableFunction {
    pub function: Arc<paro_function::table::TableFunction>,
    pub alias: String,
    pub column_names: Vec<String>,
    pub column_types: Vec<LogicalType>,
    pub table_index: usize,
    pub bound_arguments: Vec<Expression>,
    pub input_table_types: Vec<LogicalType>,
    pub input_table_names: Vec<String>,
    pub is_in_out_function: bool,
    pub child_table: Option<Box<BoundFromItem>>,
    pub with_ordinality: bool,
}
