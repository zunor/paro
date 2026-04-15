// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::binder::ir::BoundStatementKind;
use crate::binder::Binder;
use paro_common::error::Result;
use paro_parser::ast::DropPropertyGraphStmt;

#[derive(Debug, Clone)]
pub struct BoundDropPropertyGraphInfo {
    pub catalog_name: String,
    pub schema_name: String,
    pub graph_name: String,
    pub if_exists: bool,
}

pub fn bind_drop_property_graph(
    binder: &mut Binder,
    stmt: DropPropertyGraphStmt,
) -> Result<BoundStatementKind> {
    let catalog_name = binder.catalog().name().to_string();
    let schema_name = binder.session_context().current_schema().to_string();
    let graph_name = stmt.graph_name.name;
    let txn = binder.catalog_txn_view();
    let schema = binder.catalog().get_schema(&txn, &schema_name)?;

    match schema.get_property_graph(&txn, &graph_name) {
        Ok(_) => {}
        Err(_err) if stmt.if_exists => {
            return Ok(BoundStatementKind::DropPropertyGraph(
                BoundDropPropertyGraphInfo {
                    catalog_name,
                    schema_name,
                    graph_name,
                    if_exists: true,
                },
            ));
        }
        Err(err) => return Err(err),
    }

    Ok(BoundStatementKind::DropPropertyGraph(
        BoundDropPropertyGraphInfo {
            catalog_name,
            schema_name,
            graph_name,
            if_exists: stmt.if_exists,
        },
    ))
}
