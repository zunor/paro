use crate::binder::ir::BoundStatementKind;
use crate::binder::Binder;
use paro_common::error::Result;
use paro_parser::ast::CreateSchemaStmt;

#[derive(Debug, Clone)]
pub struct BoundCreateSchemaInfo {
    pub database_name: String,
    pub schema_name: String,
    pub if_not_exists: bool,
}

pub fn bind_create_schema(
    binder: &mut Binder,
    stmt: CreateSchemaStmt,
) -> Result<BoundStatementKind> {
    // Extract database and schema name from SchemaRef
    let database_name = stmt
        .schema
        .database
        .map(|c| c.name)
        .unwrap_or_else(|| binder.catalog().name().to_string());
    let schema_name = stmt.schema.schema.name.clone();

    let if_not_exists = matches!(
        stmt.create_option,
        paro_parser::ast::CreateOption::CreateIfNotExists
    );

    if database_name != binder.catalog().name() {
        return Err(paro_common::error::not_implemented(format!(
            "Cross-database CREATE SCHEMA ({})",
            database_name
        )));
    }

    Ok(BoundStatementKind::CreateSchema(BoundCreateSchemaInfo {
        database_name,
        schema_name,
        if_not_exists,
    }))
}
