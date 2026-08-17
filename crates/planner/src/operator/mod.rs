// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Logical Operator Module
//!
//!

mod column_binding;
mod operator_type;
mod plan_operator;
mod projection_map;

pub mod aggregate;
pub mod alter;
pub mod copy_to;
pub mod create_index;
pub mod create_property_graph;
pub mod create_routine;
pub mod create_schema;
pub mod create_sequence;
pub mod create_table;
pub mod create_view;
pub mod cte;
pub mod delete;
pub mod delim_get;
pub mod dependent_join;
pub mod distinct;
pub mod drop;
pub mod drop_property_graph;
pub mod empty_result;
pub mod explain;
pub mod expression_get;
pub mod external_project;
pub mod external_table;
pub mod filter;
pub mod get;
pub mod graph_expand;
pub mod graph_match;
pub mod graph_scan;
pub mod insert;
pub mod join;
pub mod limit;
pub mod order;
pub mod projection;
pub mod refresh_property_graph;
pub mod row_fetch;
pub mod search_scan;
pub mod set_operation;
pub mod table_function;
pub mod topn;
pub mod update;
pub mod window;

pub use self::plan_operator::LogicalOperator;
pub use aggregate::{Aggregate, PostAggregateReduction};
pub use alter::Alter;
pub use column_binding::ColumnBinding;
pub use copy_to::CopyTo;
pub use create_index::CreateIndex;
pub use create_property_graph::CreatePropertyGraph;
pub use create_routine::CreateRoutine;
pub use create_schema::CreateSchema;
pub use create_sequence::CreateSequence;
pub use create_table::CreateTable;
pub use create_view::CreateView;
pub use cte::{CTERef, MaterializedCTE, RecursiveCTE};
pub use delete::Delete;
pub use delim_get::DelimGet;
pub use dependent_join::{AnyAllPayload, DependentJoin, DependentJoinKind, MarkSubqueryKind};
pub use distinct::{Distinct, DistinctType};
pub use drop::Drop;
pub use drop_property_graph::DropPropertyGraph;
pub use empty_result::EmptyResult;
pub use explain::{Explain, ExplainDetail, ExplainFormat, ExplainMode, ExplainSpec};
pub use expression_get::ExpressionGet;
pub use external_project::LogicalExternalProject;
pub use external_table::LogicalExternalTable;
pub use filter::Filter;
pub use get::{Get, GetColumnProjection};
pub use graph_expand::{ExpandDirection, GraphExpand};
pub use graph_match::GraphMatch;
pub use graph_scan::GraphScan;
pub use insert::{Insert, InsertOnConflict, InsertOnConflictAction};
pub use join::{
    AntiJoinMode, AnyJoin, ComparisonJoin, CrossProduct, Join, JoinComparisonType, JoinCondition,
    JoinSide, JoinType, MarkJoinSemantics,
};
pub use limit::Limit;
pub use operator_type::LogicalOperatorType;
pub use order::Order;
pub use projection::Projection;
pub use projection_map::ProjectionMap;
pub use refresh_property_graph::RefreshPropertyGraph;
pub use row_fetch::{RowFetch, RowFetchSource};
pub use search_scan::{
    analyze_fulltext_query_stats, build_fulltext_query_stats, normalize_fulltext_config,
    Confidence, FullTextFilterScan, FullTextQueryKind, FullTextQueryStats, FullTextQueryStatsKind,
    FullTextScoreMode, SearchCandidate, SearchDecision, SearchScan,
};
pub use set_operation::{SetOpType, SetOperation};
pub use table_function::TableFunctionGet;
pub use topn::TopN;
pub use update::Update;
pub use window::Window;
