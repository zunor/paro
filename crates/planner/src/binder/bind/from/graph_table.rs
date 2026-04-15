// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::binder::bind::expr::bind_expression;
use crate::binder::bind::graph::{BoundPatternElement, GraphBindContext};
use crate::binder::ir::{BoundFromGraphTable, BoundFromItem, BoundGraphColumn, BoundGraphPattern};
use crate::binder::Binder;
use crate::expression::{ColumnRefExpression, Expression};
use crate::operator::ColumnBinding;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_parser::ast::{Expr, GraphTableRef, PatternElement, TableAlias};

/// Well-known path function names recognized in GRAPH_TABLE COLUMNS.
const PATH_FUNCTION_NAMES: &[&str] = &["path_length", "vertices", "edges", "element_id"];
const PATH_LENGTH_OFFSET: usize = 0;
const PATH_VERTICES_OFFSET: usize = 1;
const PATH_EDGES_OFFSET: usize = 2;

pub fn bind_graph_table(
    binder: &mut Binder,
    graph_table_ref: GraphTableRef,
    alias: Option<TableAlias>,
) -> Result<BoundFromItem> {
    let txn = binder.catalog_txn_view();
    let schema_name = binder.session_context().current_schema().to_string();
    let graph_name = graph_table_ref.graph_name.name.clone();

    let schema = binder.catalog().get_schema(&txn, &schema_name)?;
    let graph_entry = schema.get_property_graph(&txn, &graph_name).map_err(|_| {
        paro_error::catalog(format!("Property graph \"{}\" does not exist", graph_name))
    })?;

    let (bound_pattern, bound_columns, output_names, output_types, has_path_functions, path_length_col_idx) =
        binder.with_overlay_scope(|binder| -> Result<_> {
            let mut graph_ctx = GraphBindContext::new(graph_entry.clone());

            for path in &graph_table_ref.match_clause.pattern.paths {
                if path.elements.is_empty() {
                    return Err(paro_error::catalog("Graph path pattern cannot be empty"));
                }

                let mut idx = 0usize;
                let first_vertex = match &path.elements[idx] {
                    PatternElement::Vertex(v) => graph_ctx.bind_vertex(binder, v)?,
                    _ => {
                        return Err(paro_error::catalog(
                            "Graph path pattern must start with a vertex element",
                        ));
                    }
                };
                idx += 1;
                let mut current_vertex_var = first_vertex.variable_name;

                while idx < path.elements.len() {
                    let edge = match &path.elements[idx] {
                        PatternElement::Edge(e) => e,
                        _ => {
                            return Err(paro_error::catalog(
                                "Graph path pattern must alternate vertex and edge elements",
                            ));
                        }
                    };
                    idx += 1;
                    if idx >= path.elements.len() {
                        return Err(paro_error::catalog(
                            "Graph path pattern edge must be followed by a vertex",
                        ));
                    }
                    let next_vertex = match &path.elements[idx] {
                        PatternElement::Vertex(v) => graph_ctx.bind_vertex(binder, v)?,
                        _ => {
                            return Err(paro_error::catalog(
                                "Graph path pattern must alternate vertex and edge elements",
                            ));
                        }
                    };
                    idx += 1;

                    let bound_edge = graph_ctx.bind_edge(
                        binder,
                        edge,
                        &current_vertex_var,
                        &next_vertex.variable_name,
                    )?;
                    let _ = bound_edge;
                    graph_ctx.swap_last_two_elements();
                    current_vertex_var = next_vertex.variable_name;
                }
            }

            if let Some(match_where) = &graph_table_ref.match_clause.where_clause {
                let _ = bind_expression(binder, (**match_where).clone())?;
            }

            graph_ctx.bind_columns(&graph_table_ref.columns)?;

            let path_variable = graph_table_ref
                .match_clause
                .path_variable
                .as_ref()
                .map(|v| v.name.clone());
            let has_path_functions = graph_table_ref
                .columns
                .iter()
                .any(|col| is_path_function_call(&col.expr, &path_variable));

            let num_edges = graph_ctx
                .pattern_chain()
                .iter()
                .filter(|e| {
                    matches!(
                        e,
                        BoundPatternElement::Edge(_)
                    )
                })
                .count();
            let expand_chain_cols = 2 + 3 * num_edges;
            let path_length_col_idx = has_path_functions.then_some(expand_chain_cols);

            let mut bound_columns = Vec::with_capacity(graph_table_ref.columns.len());
            let mut output_names = Vec::with_capacity(graph_table_ref.columns.len());
            let mut output_types = Vec::with_capacity(graph_table_ref.columns.len());

            for col in &graph_table_ref.columns {
                if let Some((func_name, _arg_name)) =
                    extract_path_function_call(&col.expr, &path_variable)
                {
                    let (expr, logical_type) = match func_name.as_str() {
                        "path_length" => {
                            let binding = ColumnBinding::new(
                                usize::MAX,
                                expand_chain_cols + PATH_LENGTH_OFFSET,
                            );
                            let expr = Expression::ColumnRef(ColumnRefExpression::new(
                                binding,
                                LogicalType::BigInt,
                            ));
                            (expr, LogicalType::BigInt)
                        }
                        "element_id" => {
                            return Err(paro_error::not_implemented(
                                "element_id() is only valid for vertex or edge variables, not for a path variable",
                            ));
                        }
                        "vertices" => {
                            let logical_type = path_element_list_type();
                            let binding = ColumnBinding::new(
                                usize::MAX,
                                expand_chain_cols + PATH_VERTICES_OFFSET,
                            );
                            let expr = Expression::ColumnRef(ColumnRefExpression::new(
                                binding,
                                logical_type.clone(),
                            ));
                            (expr, logical_type)
                        }
                        "edges" => {
                            let logical_type = path_element_list_type();
                            let binding = ColumnBinding::new(
                                usize::MAX,
                                expand_chain_cols + PATH_EDGES_OFFSET,
                            );
                            let expr = Expression::ColumnRef(ColumnRefExpression::new(
                                binding,
                                logical_type.clone(),
                            ));
                            (expr, logical_type)
                        }
                        _ => unreachable!(),
                    };
                    let alias_name = col
                        .alias
                        .as_ref()
                        .map(|a| a.name.clone())
                        .unwrap_or_else(|| col.expr.to_string());
                    output_names.push(alias_name.clone());
                    output_types.push(logical_type.clone());
                    bound_columns.push(BoundGraphColumn {
                        expr,
                        alias: alias_name,
                        logical_type,
                    });
                } else {
                    let expr = bind_expression(binder, col.expr.clone())?;
                    let logical_type = expr.return_type();
                    let alias_name = col
                        .alias
                        .as_ref()
                        .map(|a| a.name.clone())
                        .unwrap_or_else(|| col.expr.to_string());
                    output_names.push(alias_name.clone());
                    output_types.push(logical_type.clone());
                    bound_columns.push(BoundGraphColumn {
                        expr,
                        alias: alias_name,
                        logical_type,
                    });
                }
            }

            Ok((
                BoundGraphPattern {
                    elements: graph_ctx.into_pattern_chain(),
                },
                bound_columns,
                output_names,
                output_types,
                has_path_functions,
                path_length_col_idx,
            ))
        })?;

    let table_index = binder.bind_context.generate_table_index();
    let binding_alias = alias
        .as_ref()
        .map(|a| a.name.name.clone())
        .unwrap_or_else(|| graph_name.clone());

    binder.bind_context.add_binding(
        binding_alias,
        table_index,
        output_names.clone(),
        output_types.clone(),
    );

    Ok(BoundFromItem::GraphTable(BoundFromGraphTable {
        graph_entry,
        bound_pattern,
        bound_columns,
        table_index,
        output_names,
        output_types,
        path_mode: graph_table_ref.match_clause.path_mode.clone(),
        has_path_functions,
        path_length_col_idx,
    }))
}

/// Check if an expression is a path function call (path_length, vertices, edges, element_id).
fn is_path_function_call(expr: &Expr, path_variable: &Option<String>) -> bool {
    extract_path_function_call(expr, path_variable).is_some()
}

/// Extract path function name and argument name from an expression.
/// Returns Some((func_name, arg_name)) if the expression is a recognized path function call.
fn extract_path_function_call(
    expr: &Expr,
    path_variable: &Option<String>,
) -> Option<(String, String)> {
    if let Expr::FunctionCall { func, .. } = expr {
        let func_name = func.name.name.to_lowercase();
        if PATH_FUNCTION_NAMES.contains(&func_name.as_str()) && func.args.len() == 1 {
            // The argument should be a column reference to the path variable
            if let Expr::ColumnRef { column, .. } = &func.args[0] {
                let arg_name = column.column.name().to_string();
                // If there's a path variable, check it matches
                if let Some(pv) = path_variable {
                    if arg_name == *pv {
                        return Some((func_name, arg_name));
                    }
                }
                // Also accept if no explicit path variable (implicit path)
                if path_variable.is_none() {
                    return Some((func_name, arg_name));
                }
            }
        }
    }
    None
}

fn path_element_list_type() -> LogicalType {
    LogicalType::List(Box::new(LogicalType::Struct(vec![
        ("table_oid".to_string(), LogicalType::UBigInt),
        ("rowid".to_string(), LogicalType::UBigInt),
    ])))
}
