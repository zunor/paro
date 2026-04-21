// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Physical-plan lowering from logical operators to execution operators.

pub mod generator;
pub mod plan_aggregate;
pub mod plan_alter;
pub mod plan_copy_to;
pub mod plan_create_index;
pub mod plan_create_routine;
pub mod plan_create_sequence;
pub mod plan_create_view;
pub mod plan_cte;
pub mod plan_delete;
pub mod plan_delim_get;
pub mod plan_delim_join;
pub mod plan_distinct;
pub mod plan_drop;
pub mod plan_empty_result;
pub mod plan_explain;
pub mod plan_expression_get;
pub mod plan_external_project;
pub mod plan_external_table;
pub mod plan_filter;
pub mod plan_get;
pub mod plan_graph;
pub mod plan_insert;
pub mod plan_join;
pub mod plan_limit;
pub mod plan_order;
pub mod plan_projection;
pub mod plan_search;
pub mod plan_set_operation;
pub mod plan_table_function;
pub mod plan_topn;
pub mod plan_update;
pub mod plan_window;
pub mod predicate_builder;
