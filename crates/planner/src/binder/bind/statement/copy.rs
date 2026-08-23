// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Bind COPY Statement
//!
//!

use std::sync::Arc;

use crate::binder::bind::clause::WhereBinder;
use crate::binder::ir::BoundFromItem;
use crate::binder::ir::BoundStatementKind;
use crate::binder::Binder;
use crate::expression::Expression;
use crate::operator::{CopyTo, Filter, Insert, LogicalOperator, TableFunctionGet};
use paro_catalog::entry::{CatalogEntryEnum, CatalogType};
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_function::copy::{CopyFormat, CopyFromSource, CopyOptions};
use paro_function::table::BoundTableFunctionData;
use paro_parser::ast::{
    ColumnID, ColumnRef, CopyDirection, CopySource, CopyStmt, CopyTarget, Expr, Identifier,
    Indirection, Query, SelectStmt, SelectTarget, SetExpr, TableReference,
};
use paro_parser::Span;

/// Bound information for COPY statements.
#[derive(Debug)]
pub struct BoundCopyInfo {
    pub plan: LogicalOperator,
    pub names: Vec<String>,
    pub types: Vec<LogicalType>,
}

pub fn bind_copy(binder: &mut Binder, stmt: CopyStmt) -> Result<BoundStatementKind> {
    match stmt.direction {
        CopyDirection::To => bind_copy_to(binder, stmt),
        CopyDirection::From => bind_copy_from(binder, stmt),
    }
}

fn bind_copy_to(binder: &mut Binder, stmt: CopyStmt) -> Result<BoundStatementKind> {
    if stmt.where_clause.is_some() {
        return Err(paro_error::syntax("COPY TO does not support WHERE clause"));
    }

    let source_path = match &stmt.source {
        CopySource::File(path) => path.clone(),
        CopySource::Stdout => {
            return Err(paro_error::not_implemented(
                "COPY TO STDOUT is not supported yet",
            ))
        }
        CopySource::Stdin => {
            return Err(paro_error::syntax("COPY TO cannot use STDIN"));
        }
        CopySource::Program(_) => {
            return Err(paro_error::not_implemented(
                "COPY TO PROGRAM is not supported",
            ))
        }
    };

    let query = match stmt.target {
        CopyTarget::Table { name, columns } => build_select_query(name, columns),
        CopyTarget::Query(query) => *query,
    };

    let bound_query = binder.bind_query(query)?;
    let input_names = bound_query.names();
    let input_types = bound_query.types();

    let child_plan = binder.plan_query(bound_query)?;

    let options = CopyOptions::from_ast(&stmt.options)?;
    let format_name = match options.format {
        CopyFormat::Csv => "csv",
        CopyFormat::Text => "text",
        CopyFormat::Ndjson => "ndjson",
        CopyFormat::Binary => {
            return Err(paro_error::not_implemented(
                "COPY TO BINARY is not supported yet",
            ))
        }
    };

    let copy_function = lookup_copy_function(binder, format_name)?;
    let bind_data = (copy_function.copy_to_bind)(&options, &input_names, &input_types)?;
    let bind_data: Arc<dyn paro_function::copy::CopyFunctionBindData> = bind_data.into();

    let output_names = vec!["count".to_string()];
    let output_types = vec![LogicalType::BigInt];

    let copy_to = CopyTo::new(
        copy_function,
        bind_data,
        source_path,
        stmt.source,
        options,
        binder.wrap_plan(child_plan),
        output_names.clone(),
        output_types.clone(),
    );

    Ok(BoundStatementKind::Copy(BoundCopyInfo {
        plan: LogicalOperator::CopyTo(copy_to),
        names: output_names,
        types: output_types,
    }))
}

fn bind_copy_from(binder: &mut Binder, stmt: CopyStmt) -> Result<BoundStatementKind> {
    let where_clause = stmt.where_clause;

    let (table_ref, columns) = match stmt.target {
        CopyTarget::Table { name, columns } => (name, columns),
        CopyTarget::Query(_) => {
            return Err(paro_error::syntax(
                "COPY FROM does not support query sources",
            ))
        }
    };

    let source = match &stmt.source {
        CopySource::File(path) => CopyFromSource::File(path.clone()),
        CopySource::Stdin => CopyFromSource::Stdin,
        CopySource::Stdout => {
            return Err(paro_error::syntax("COPY FROM cannot use STDOUT"));
        }
        CopySource::Program(_) => {
            return Err(paro_error::not_implemented(
                "COPY FROM PROGRAM is not supported",
            ))
        }
    };

    let copy_target_alias = table_ref.table.name.clone();

    let table_ref_ast = TableReference::Table {
        span: None,
        database: table_ref.database.clone(),
        schema: table_ref.schema.clone(),
        table: table_ref.table.clone(),
        alias: None,
        temporal: None,
        with_options: table_ref.with_options.clone(),
        pivot: None,
        unpivot: None,
        sample: None,
    };

    let bound_table = binder.bind_table_ref(table_ref_ast)?;
    let bound_base_table = match bound_table {
        BoundFromItem::BaseTable(bt) => bt,
        _ => return Err(paro_error::not_implemented("COPY FROM non-base table")),
    };

    let table = bound_base_table.table;

    let mut column_indices = Vec::new();
    let mut column_names = Vec::new();
    let mut expected_types = Vec::new();

    if let Some(columns) = columns {
        for col_ident in columns {
            let col_name = &col_ident.name;
            let found = table
                .columns
                .iter()
                .enumerate()
                .find(|(_, c)| c.name.eq_ignore_ascii_case(col_name));
            if let Some((idx, col)) = found {
                column_indices.push(idx);
                column_names.push(col.name.clone());
                expected_types.push(col.logical_type.clone());
            } else {
                return Err(paro_error::catalog(format!(
                    "Column {} not found in table {}",
                    col_name,
                    table.name()
                )));
            }
        }
    } else {
        for (idx, col) in table.columns.iter().enumerate() {
            column_indices.push(idx);
            column_names.push(col.name.clone());
            expected_types.push(col.logical_type.clone());
        }
    }

    let options = CopyOptions::from_ast(&stmt.options)?;
    let format_name = match options.format {
        CopyFormat::Csv => "csv",
        CopyFormat::Text => "text",
        CopyFormat::Ndjson => "ndjson",
        CopyFormat::Binary => "binary",
    };

    let copy_function = lookup_copy_function(binder, format_name)?;
    let bind_data =
        (copy_function.copy_from_bind)(source, &options, &column_names, &expected_types)?;

    let table_index = binder.bind_context.generate_table_index();
    let table_function = Arc::new(copy_function.copy_from_function.clone());

    let table_function_get = TableFunctionGet::new(
        table_function,
        table_index,
        column_names.clone(),
        expected_types.clone(),
        Vec::new(),
    )
    .with_bind_data(BoundTableFunctionData::new(bind_data));

    let mut source = LogicalOperator::TableFunctionGet(table_function_get);
    if let Some(expr) = where_clause {
        let condition = bind_copy_from_where_clause(
            binder,
            *expr,
            &copy_target_alias,
            table_index,
            &column_names,
            &expected_types,
        )?;
        source = LogicalOperator::Filter(Filter::new(binder.wrap_plan(source), vec![condition]));
    }

    let insert = Insert::new(
        table,
        column_indices,
        expected_types.clone(),
        None,
        binder.wrap_plan(source),
    );

    let output_names = vec!["count".to_string()];
    let output_types = vec![LogicalType::BigInt];

    Ok(BoundStatementKind::Copy(BoundCopyInfo {
        plan: LogicalOperator::Insert(insert),
        names: output_names,
        types: output_types,
    }))
}

fn bind_copy_from_where_clause(
    binder: &mut Binder,
    where_clause: Expr,
    table_alias: &str,
    table_index: usize,
    column_names: &[String],
    column_types: &[LogicalType],
) -> Result<Expression> {
    let mut where_binder_ctx = binder.create_child();
    where_binder_ctx.bind_context.add_binding(
        table_alias.to_string(),
        table_index,
        column_names.to_vec(),
        column_types.to_vec(),
    );

    let mut where_binder = WhereBinder::new(&mut where_binder_ctx);
    where_binder.bind(where_clause)
}

fn lookup_copy_function(binder: &Binder, name: &str) -> Result<paro_function::copy::CopyFunction> {
    let search_path = binder.session_context().search_path();
    let mut entry = None;

    for search_entry in search_path {
        let catalog_name = if search_entry.catalog.is_empty() {
            binder.catalog().name().to_string()
        } else {
            search_entry.catalog.clone()
        };

        let catalog = if catalog_name == binder.catalog().name() {
            Some(binder.catalog())
        } else {
            binder
                .session_context()
                .database(&catalog_name)
                .map(|db| db.catalog.clone())
        };

        if let Some(catalog) = catalog {
            if let Ok(schema) = catalog.get_schema(&binder.catalog_txn_view(), &search_entry.schema)
            {
                if let Some(e) = schema
                    .collection(CatalogType::CopyFunction)
                    .expect("copy function collection")
                    .get_entry(
                        binder.catalog_txn_view().transaction_id,
                        binder.catalog_txn_view().start_time,
                        name,
                    )
                {
                    entry = Some(e);
                    break;
                }
            }
        }
    }

    let entry =
        entry.ok_or_else(|| paro_error::catalog(format!("Copy function '{}' not found", name)))?;

    let copy_entry = match entry.as_ref() {
        CatalogEntryEnum::CopyFunction(copy) => copy.clone(),
        _ => {
            return Err(paro_error::catalog(format!(
                "'{}' is not a COPY function",
                name
            )))
        }
    };

    Ok(copy_entry.function.clone())
}

fn build_select_query(name: paro_parser::ast::TableRef, columns: Option<Vec<Identifier>>) -> Query {
    let select_list = match columns {
        Some(cols) if !cols.is_empty() => cols
            .into_iter()
            .map(|ident| SelectTarget::AliasedExpr {
                expr: Box::new(Expr::ColumnRef {
                    span: default_span(),
                    column: ColumnRef {
                        schema: None,
                        table: None,
                        column: ColumnID::Name(ident),
                    },
                }),
                alias: None,
            })
            .collect(),
        _ => vec![SelectTarget::StarColumns {
            qualified: vec![Indirection::Star(default_span())],
            column_filter: None,
        }],
    };

    let table_ref = TableReference::Table {
        span: default_span(),
        database: name.database,
        schema: name.schema,
        table: name.table,
        alias: None,
        temporal: None,
        with_options: name.with_options,
        pivot: None,
        unpivot: None,
        sample: None,
    };

    let select_stmt = SelectStmt {
        span: default_span(),
        hints: None,
        distinct: false,
        top_n: None,
        select_list,
        from: vec![table_ref],
        selection: None,
        group_by: None,
        having: None,
        window_list: None,
        qualify: None,
    };

    Query {
        span: default_span(),
        with: None,
        body: SetExpr::Select(Box::new(select_stmt)),
        order_by: vec![],
        limit: vec![],
        offset: None,
        locking: None,
        ignore_result: false,
    }
}

fn default_span() -> Span {
    Span::default()
}
