// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Binds `CREATE INDEX` statements.

use crate::binder::bind::expr::IndexBinder;
use crate::binder::ir::BoundStatementKind;
use crate::binder::Binder;
use crate::expression::Expression;
use paro_catalog::entry::CatalogEntry;
use paro_catalog::entry::{CreateIndexInfo, IndexType, LogicalIndex, TableCatalogEntry};
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_parser::ast::{ColumnID, ColumnRef, CreateIndexStmt, Expr, IndexKind};
use paro_storage::index::fulltext::tokenizer::TokenizerKind;
use paro_storage::index::hnsw::HnswConfig;
use paro_storage::index::IndexConstraintType;
use paro_storage::search::HnswInlineThreshold;
use serde_json::{json, Value as JsonValue};
use std::collections::BTreeMap;
use std::sync::Arc;

const SIMPLE_CONFIG: &str = "simple";

/// Bound information for CREATE INDEX statement
#[derive(Debug, Clone)]
pub struct BoundCreateIndexInfo {
    /// The CreateIndexInfo containing all index metadata
    pub info: CreateIndexInfo,
    /// Reference to the target table
    pub table: Arc<TableCatalogEntry>,
    /// Bound expressions for the index columns
    pub expressions: Vec<Expression>,
    /// Original SQL statement
    pub sql: String,
}

impl BoundCreateIndexInfo {
    /// Get the column IDs from the bound expressions
    pub fn get_column_ids(&self) -> Vec<usize> {
        IndexBinder::get_column_ids(&self.expressions)
    }
}

fn unwrap_casts(expr: &Expression) -> &Expression {
    match expr {
        Expression::Cast(cast) => unwrap_casts(cast.child.as_ref()),
        _ => expr,
    }
}

fn extract_column_binding(expr: &Expression) -> Option<(LogicalIndex, LogicalType)> {
    let Expression::ColumnRef(col_ref) = unwrap_casts(expr) else {
        return None;
    };
    let idx = u32::try_from(col_ref.binding.column_index).ok()?;
    Some((LogicalIndex::new(idx), col_ref.return_type.clone()))
}

fn extract_string_literal(expr: &Expression) -> Option<String> {
    let Expression::Constant(constant) = unwrap_casts(expr) else {
        return None;
    };
    match &constant.value {
        Value::Varchar(v) => Some(v.clone()),
        _ => None,
    }
}

fn normalize_fulltext_config(config: &str) -> Result<String> {
    let kind = TokenizerKind::from_config(config)?;
    Ok(kind.config_name().to_string())
}

fn extract_fulltext_binding(expr: &Expression) -> Result<(LogicalIndex, LogicalType, String)> {
    let expr = unwrap_casts(expr);
    if let Some((column_id, column_type)) = extract_column_binding(expr) {
        return Ok((column_id, column_type, SIMPLE_CONFIG.to_string()));
    }

    let Expression::Function(func) = expr else {
        return Err(paro_error::invalid_input(
            "Full-text index expression must be a column reference or to_tsvector(config, column)",
        ));
    };
    if !func.function.name.eq_ignore_ascii_case("to_tsvector") {
        return Err(paro_error::invalid_input(format!(
            "Unsupported full-text index expression function '{}'; expected to_tsvector",
            func.function.name
        )));
    }

    let (config, text_expr) = match func.children.as_slice() {
        [text_expr] => (SIMPLE_CONFIG.to_string(), text_expr),
        [config_expr, text_expr] => {
            let raw_config = extract_string_literal(config_expr).ok_or_else(|| {
                paro_error::invalid_input(
                    "to_tsvector config must be a string literal in CREATE INDEX",
                )
            })?;
            (normalize_fulltext_config(&raw_config)?, text_expr)
        }
        _ => {
            return Err(paro_error::invalid_input(
                "to_tsvector in CREATE INDEX expects one or two arguments",
            ))
        }
    };

    let (column_id, column_type) = extract_column_binding(text_expr).ok_or_else(|| {
        paro_error::invalid_input("to_tsvector second argument in CREATE INDEX must be a column")
    })?;

    Ok((column_id, column_type, config))
}

fn resolve_index_type(stmt: &CreateIndexStmt) -> Result<IndexType> {
    if let Some(method) = &stmt.using_method {
        if method.name.eq_ignore_ascii_case("GIN") {
            return Ok(IndexType::FullText);
        }
        return Err(paro_error::not_supported(format!(
            "CREATE INDEX USING {} is not supported (only USING GIN is supported)",
            method.name
        )));
    }

    let vector_mode = stmt
        .index_options
        .iter()
        .find_map(|(k, v)| {
            let key = k.to_ascii_lowercase();
            match key.as_str() {
                "mode" | "kind" | "type" | "index_type" | "vector_mode" => {
                    Some(v.to_ascii_lowercase())
                }
                _ => None,
            }
        })
        .unwrap_or_else(|| "hnsw".to_string());

    match stmt.index_kind {
        None => Ok(IndexType::ART),
        Some(IndexKind::Vector) => match vector_mode.as_str() {
            "hnsw" => Ok(IndexType::HNSW),
            "sparse" | "sparse_vector" => Ok(IndexType::Sparse),
            other => Err(paro_error::invalid_input(format!(
                "Unsupported VECTOR index mode '{}', expected hnsw or sparse",
                other
            ))),
        },
        Some(IndexKind::Inverted) | Some(IndexKind::Ngram) => Ok(IndexType::FullText),
        Some(IndexKind::Aggregating) => Err(paro_error::not_supported(
            "AGGREGATING INDEX is not yet implemented",
        )),
    }
}

fn validate_art_index_definition(
    binder: &Binder,
    table: &TableCatalogEntry,
    schema_name: &str,
    index_name: &str,
    column_ids: &[LogicalIndex],
    is_unique: bool,
) -> Result<()> {
    if is_unique {
        return Err(paro_error::not_supported(
            "UNIQUE ART INDEX is not supported; use PRIMARY KEY for uniqueness constraints",
        ));
    }

    if column_ids.len() != 1 {
        return Err(paro_error::invalid_input(
            "ART index supports only a single column; use a composite primary key or dedicated index type for multi-column needs",
        ));
    }

    let target_column_id = column_ids[0].index as usize;
    let target_column_name = table
        .columns
        .get(target_column_id)
        .map(|column| column.name.as_str())
        .unwrap_or("<unknown>");
    let schema = binder
        .catalog()
        .get_schema(&binder.catalog_txn_view(), schema_name)?;

    for existing in schema.indexes_for_table(&binder.catalog_txn_view(), table.base.base.object_id)
    {
        if existing.name() == index_name || existing.index_type != IndexType::ART {
            continue;
        }
        if existing.column_ids.len() == 1 && existing.column_ids[0] == column_ids[0] {
            return Err(paro_error::invalid_input(format!(
                "ART index '{}' already exists on column '{}'; drop it before creating another ART index on the same column",
                existing.name(),
                target_column_name
            )));
        }
    }

    Ok(())
}

fn validate_sparse_index_definition(column_types: &[LogicalType]) -> Result<()> {
    if column_types.len() != 1 {
        return Err(paro_error::invalid_input(
            "Sparse vector index supports only a single binary sparse row image column",
        ));
    }
    if !matches!(column_types[0], LogicalType::Blob) {
        return Err(paro_error::not_supported(format!(
            "Sparse vector index requires Blob binary sparse row image input; use sparse_vector(...) to materialize source text first, got {:?}",
            column_types[0]
        )));
    }
    Ok(())
}

fn validate_hnsw_index_definition(
    binder: &Binder,
    table: &TableCatalogEntry,
    schema_name: &str,
    index_name: &str,
    column_ids: &[LogicalIndex],
) -> Result<()> {
    let [column_id] = column_ids else {
        return Err(paro_error::invalid_input(
            "HNSW index requires exactly one VECTOR(N) column",
        ));
    };
    let schema = binder
        .catalog()
        .get_schema(&binder.catalog_txn_view(), schema_name)?;
    for existing in schema.indexes_for_table(&binder.catalog_txn_view(), table.base.base.object_id)
    {
        if existing.name() == index_name || existing.index_type != IndexType::HNSW {
            continue;
        }
        if existing.column_ids.as_slice() == [*column_id] {
            return Err(paro_error::invalid_input(format!(
                "HNSW index '{}' already exists on column {}; one physical HNSW contract per column is supported",
                existing.name(), column_id.index
            )));
        }
    }
    Ok(())
}

fn parse_u64_index_option(
    options: &BTreeMap<String, String>,
    name: &str,
    default: u64,
) -> Result<u64> {
    options.get(name).map_or(Ok(default), |value| {
        value.parse::<u64>().map_err(|_| {
            paro_error::invalid_input(format!(
                "HNSW index option {name} must be a non-negative integer, got '{value}'"
            ))
        })
    })
}

fn hnsw_provider_config(
    options: &BTreeMap<String, String>,
    column_types: &[LogicalType],
) -> Result<JsonValue> {
    let [LogicalType::Array(inner, dimension)] = column_types else {
        return Err(paro_error::not_supported(
            "HNSW index requires exactly one VECTOR(N) column",
        ));
    };
    if !matches!(inner.as_ref(), LogicalType::Float) {
        return Err(paro_error::not_supported(
            "HNSW index requires exactly one VECTOR(N) column",
        ));
    }

    const TYPE_KEYS: &[&str] = &["mode", "kind", "type", "index_type", "vector_mode"];
    const HNSW_KEYS: &[&str] = &[
        "m",
        "ef_construct",
        "plain_scan_threshold",
        "filtered_plain_scan_threshold",
        "inline_max_vector_count",
        "inline_max_graph_memory_bytes",
        "inline_max_dimension",
    ];
    if let Some(unknown) = options
        .keys()
        .find(|key| !TYPE_KEYS.contains(&key.as_str()) && !HNSW_KEYS.contains(&key.as_str()))
    {
        return Err(paro_error::invalid_input(format!(
            "Unknown HNSW index option '{unknown}'"
        )));
    }

    let defaults = HnswConfig::default();
    let inline_defaults = HnswInlineThreshold::DEFAULT;
    let m = parse_u64_index_option(options, "m", defaults.m as u64)?;
    if !(2..=1_024).contains(&m) {
        return Err(paro_error::invalid_input(format!(
            "HNSW index option m must be between 2 and 1024, got {m}"
        )));
    }
    let ef_construct =
        parse_u64_index_option(options, "ef_construct", defaults.ef_construct as u64)?;
    if ef_construct < m || ef_construct > 1_000_000 {
        return Err(paro_error::invalid_input(format!(
            "HNSW index option ef_construct must be between m ({m}) and 1000000, got {ef_construct}"
        )));
    }

    let plain_scan_threshold = parse_u64_index_option(
        options,
        "plain_scan_threshold",
        defaults.plain_scan_threshold as u64,
    )?;
    let filtered_plain_scan_threshold = parse_u64_index_option(
        options,
        "filtered_plain_scan_threshold",
        defaults.filtered_plain_scan_threshold as u64,
    )?;
    let inline_max_vector_count = parse_u64_index_option(
        options,
        "inline_max_vector_count",
        inline_defaults.max_vector_count,
    )?;
    let inline_max_graph_memory_bytes = parse_u64_index_option(
        options,
        "inline_max_graph_memory_bytes",
        inline_defaults.max_graph_memory_bytes,
    )?;
    let inline_max_dimension = parse_u64_index_option(
        options,
        "inline_max_dimension",
        u64::from(inline_defaults.max_dimension),
    )?;
    if inline_max_dimension > u64::from(u32::MAX) {
        return Err(paro_error::invalid_input(format!(
            "HNSW index option inline_max_dimension exceeds {}, got {inline_max_dimension}",
            u32::MAX
        )));
    }

    Ok(json!({
        "m": m,
        "ef_construct": ef_construct,
        "distance": 0,
        "dimension": *dimension,
        "plain_scan_threshold": plain_scan_threshold,
        "filtered_plain_scan_threshold": filtered_plain_scan_threshold,
        "inline_threshold": {
            "max_vector_count": inline_max_vector_count,
            "max_graph_memory_bytes": inline_max_graph_memory_bytes,
            "max_dimension": inline_max_dimension,
        }
    }))
}

fn provider_config_for_index(
    stmt: &CreateIndexStmt,
    index_type: IndexType,
    column_types: &[LogicalType],
    fulltext_binding: Option<&(LogicalIndex, String)>,
) -> Result<JsonValue> {
    match index_type {
        IndexType::HNSW => hnsw_provider_config(&stmt.index_options, column_types),
        IndexType::Sparse => Ok(json!({ "physical_encoding": "binary-v1" })),
        IndexType::FullText => Ok(json!({
            "config": fulltext_binding
                .map(|(_, config)| config.as_str())
                .unwrap_or(SIMPLE_CONFIG)
        })),
        _ => Ok(json!({})),
    }
}

/// Bind a CREATE INDEX statement
pub fn bind_create_index(binder: &mut Binder, stmt: CreateIndexStmt) -> Result<BoundStatementKind> {
    // 1. Resolve table name
    let database_name = stmt
        .database
        .as_ref()
        .map(|i| i.name.clone())
        .unwrap_or_else(|| binder.catalog().name().to_string());
    let schema_name = stmt
        .schema
        .as_ref()
        .map(|i| i.name.clone())
        .unwrap_or_else(|| binder.session_context().current_schema().to_string());
    let table_name = stmt.table.name.clone();

    // 2. Verify database matches
    if database_name != binder.catalog().name() {
        return Err(paro_error::not_implemented(format!(
            "Cross-database CREATE INDEX ({})",
            database_name
        )));
    }

    // 3. Look up the target table
    let table_entry =
        binder
            .catalog()
            .get_table(&binder.catalog_txn_view(), &schema_name, &table_name)?;

    // Extract TableCatalogEntry from CatalogEntryEnum
    let table = match table_entry.as_ref() {
        paro_catalog::entry::CatalogEntryEnum::Table(t) => Arc::clone(t),
        _ => return Err(paro_error::wrong_object_type("table", &table_name)),
    };

    // 4. Resolve index name
    let index_name = stmt.index_name.name.clone();

    // 5. Setup IndexBinder and bind expressions
    let table_index = binder.bind_context.generate_table_index();
    let mut index_binder = IndexBinder::new(binder, Arc::clone(&table), table_index);
    index_binder.setup_bind_context();

    // 6. Build expression list (PG USING GIN path provides expressions directly)
    let expressions = if stmt.expressions.is_empty() {
        stmt.columns
            .iter()
            .map(|col_ident| Expr::ColumnRef {
                span: col_ident.span,
                column: ColumnRef {
                    schema: None,
                    table: None,
                    column: ColumnID::Name(col_ident.clone()),
                },
            })
            .collect::<Vec<_>>()
    } else {
        stmt.expressions.clone()
    };
    if expressions.is_empty() {
        return Err(paro_error::invalid_input(
            "CREATE INDEX requires at least one index key expression",
        ));
    }

    let mut bound_expressions = Vec::with_capacity(expressions.len());
    for expr in expressions {
        bound_expressions.push(index_binder.bind_expression(expr)?);
    }

    // 7. Determine index type.
    let index_type = resolve_index_type(&stmt)?;

    // 8. Extract index key metadata.
    let (column_ids, column_types, fulltext_binding) = if index_type == IndexType::FullText {
        if bound_expressions.len() != 1 {
            return Err(paro_error::invalid_input(
                "CREATE INDEX USING GIN expects exactly one index expression",
            ));
        }
        let (column_id, column_type, config) = extract_fulltext_binding(&bound_expressions[0])?;
        (
            vec![column_id],
            vec![column_type],
            Some((column_id, config)),
        )
    } else {
        let mut ids = Vec::with_capacity(bound_expressions.len());
        let mut types = Vec::with_capacity(bound_expressions.len());
        for expr in &bound_expressions {
            let Some((column_id, column_type)) = extract_column_binding(expr) else {
                return Err(paro_error::invalid_input(
                    "Only direct column references are supported for this index type",
                ));
            };
            ids.push(column_id);
            types.push(column_type);
        }
        (ids, types, None)
    };

    if index_type == IndexType::ART {
        validate_art_index_definition(
            binder,
            table.as_ref(),
            &schema_name,
            &index_name,
            &column_ids,
            stmt.is_unique,
        )?;
    }
    if index_type == IndexType::Sparse {
        validate_sparse_index_definition(&column_types)?;
    }
    if index_type == IndexType::HNSW {
        validate_hnsw_index_definition(
            binder,
            table.as_ref(),
            &schema_name,
            &index_name,
            &column_ids,
        )?;
    }
    let provider_config =
        provider_config_for_index(&stmt, index_type, &column_types, fulltext_binding.as_ref())?;

    // 9. Determine constraint type
    let _constraint_type = IndexConstraintType::None; // Default for now

    // 10. Build the original SQL
    let sql = stmt.to_string();

    // 11. Create CreateIndexInfo using the builder pattern
    let mut info = CreateIndexInfo::new(
        schema_name,
        table_name,
        index_name,
        column_ids,
        column_types,
    )
    .with_catalog(database_name)
    .with_index_type(index_type)
    .with_provider_config(provider_config)
    .with_sql(sql.clone());
    if let Some((column_id, config)) = fulltext_binding {
        info = info.with_fulltext_options(column_id, config);
    }

    let info = if matches!(
        stmt.create_option,
        paro_parser::ast::CreateOption::CreateIfNotExists
    ) {
        info.with_if_not_exists()
    } else {
        info
    };

    // 12. Return bound statement
    Ok(BoundStatementKind::CreateIndex(BoundCreateIndexInfo {
        info,
        table,
        expressions: bound_expressions,
        sql,
    }))
}

#[cfg(test)]
mod tests {
    use super::bind_create_index;
    use crate::binder::ir::BoundStatementKind;
    use crate::binder::test_utils::{test_binder, test_binder_with_public_table};
    use crate::binder::Binder;
    use paro_catalog::entry::{
        CatalogEntryEnum, CreateIndexInfo, IndexType, LogicalIndex, TableCatalogEntry,
    };
    use paro_catalog::mvcc::CatalogSnapshot;
    use paro_common::types::LogicalType;
    use paro_parser::ast::{CreateIndexStmt, Statement};
    use paro_parser::parse_one;
    use std::sync::Arc;

    fn parse_create_index_stmt(sql: &str) -> CreateIndexStmt {
        match parse_one(sql).expect("statement should parse").stmt {
            Statement::CreateIndex(stmt) => stmt,
            other => panic!("expected CREATE INDEX statement, got {other:?}"),
        }
    }

    fn fetch_public_table(binder: &Binder, table_name: &str) -> Arc<TableCatalogEntry> {
        let txn = binder.catalog_txn_view();
        let entry = binder
            .catalog()
            .get_table(&txn, "public", table_name)
            .expect("test table should exist");
        match entry.as_ref() {
            CatalogEntryEnum::Table(table) => Arc::clone(table),
            other => panic!("expected table entry, got {other:?}"),
        }
    }

    fn install_art_index(
        binder: &Binder,
        table: &TableCatalogEntry,
        index_name: &str,
        column_id: u32,
        logical_type: LogicalType,
    ) {
        let txn = CatalogSnapshot::permanent_writer(u64::MAX);
        let schema = binder
            .catalog()
            .get_schema(&txn, "public")
            .expect("public schema should exist");
        let info = CreateIndexInfo::new(
            "public".to_string(),
            table.name().to_string(),
            index_name.to_string(),
            vec![LogicalIndex::new(column_id)],
            vec![logical_type],
        )
        .with_catalog(binder.catalog().name().to_string())
        .with_index_type(IndexType::ART);
        schema
            .create_index(&txn, info, table)
            .expect("install test ART index");
    }

    #[test]
    fn bind_create_index_defaults_to_art() {
        let mut binder =
            test_binder_with_public_table("orders", &[("customer_id", LogicalType::Integer)]);

        let bound = bind_create_index(
            &mut binder,
            parse_create_index_stmt("CREATE INDEX idx_orders_customer_id ON orders (customer_id)"),
        )
        .expect("default CREATE INDEX should bind");

        let BoundStatementKind::CreateIndex(bound) = bound else {
            panic!("expected bound CREATE INDEX");
        };
        assert_eq!(bound.info.index_type, IndexType::ART);
        assert_eq!(bound.info.column_ids, vec![LogicalIndex::new(0)]);
        assert_eq!(bound.info.column_types, vec![LogicalType::Integer]);
    }

    #[test]
    fn bind_create_index_rejects_multi_column_art() {
        let mut binder = test_binder_with_public_table(
            "orders",
            &[
                ("customer_id", LogicalType::Integer),
                ("status_id", LogicalType::Integer),
            ],
        );

        let err = bind_create_index(
            &mut binder,
            parse_create_index_stmt(
                "CREATE INDEX idx_orders_customer_status ON orders (customer_id, status_id)",
            ),
        )
        .expect_err("multi-column ART index should be rejected");

        assert!(
            err.to_string()
                .contains("ART index supports only a single column"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn bind_create_index_rejects_unique_art() {
        let mut binder =
            test_binder_with_public_table("orders", &[("customer_id", LogicalType::Integer)]);

        let err = bind_create_index(
            &mut binder,
            parse_create_index_stmt(
                "CREATE UNIQUE INDEX idx_orders_customer_id ON orders (customer_id)",
            ),
        )
        .expect_err("UNIQUE ART index should be rejected");

        assert!(
            err.to_string()
                .contains("UNIQUE ART INDEX is not supported"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn bind_create_index_rejects_duplicate_art_column() {
        let mut binder =
            test_binder_with_public_table("orders", &[("customer_id", LogicalType::Integer)]);
        let table = fetch_public_table(&binder, "orders");
        install_art_index(
            &binder,
            table.as_ref(),
            "idx_orders_customer_existing",
            0,
            LogicalType::Integer,
        );

        let err = bind_create_index(
            &mut binder,
            parse_create_index_stmt("CREATE INDEX idx_orders_customer_new ON orders (customer_id)"),
        )
        .expect_err("duplicate ART column should be rejected");

        assert!(
            err.to_string().contains("idx_orders_customer_existing"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn bind_create_sparse_index_requires_blob_row_image() {
        let mut binder = test_binder_with_public_table("docs", &[("tokens", LogicalType::Varchar)]);

        let err = bind_create_index(
            &mut binder,
            parse_create_index_stmt(
                "CREATE VECTOR INDEX idx_docs_sparse ON docs (tokens) mode = sparse",
            ),
        )
        .expect_err("Sparse index over Varchar should be rejected");

        assert!(
            err.to_string()
                .contains("Blob binary sparse row image input"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn bind_create_sparse_index_accepts_blob_row_image() {
        let mut binder = test_binder_with_public_table("docs", &[("tokens", LogicalType::Blob)]);

        let bound = bind_create_index(
            &mut binder,
            parse_create_index_stmt(
                "CREATE VECTOR INDEX idx_docs_sparse ON docs (tokens) mode = sparse",
            ),
        )
        .expect("Sparse index over Blob row image should bind");

        let BoundStatementKind::CreateIndex(bound) = bound else {
            panic!("expected bound CREATE INDEX");
        };
        assert_eq!(bound.info.index_type, IndexType::Sparse);
        assert_eq!(bound.info.column_ids, vec![LogicalIndex::new(0)]);
        assert_eq!(bound.info.column_types, vec![LogicalType::Blob]);
    }

    #[test]
    fn bind_create_hnsw_index_persists_typed_provider_config() {
        let vector_type = LogicalType::Array(Box::new(LogicalType::Float), 100);
        let mut binder = test_binder_with_public_table("items", &[("embedding", vector_type)]);

        let bound = bind_create_index(
            &mut binder,
            parse_create_index_stmt(
                "CREATE VECTOR INDEX idx_items_embedding ON items (embedding) \
                 m = 32 ef_construct = 160 plain_scan_threshold = 20000 \
                 inline_max_vector_count = 90000 \
                 inline_max_graph_memory_bytes = 268435456 \
                 inline_max_dimension = 256",
            ),
        )
        .expect("HNSW options should bind");

        let BoundStatementKind::CreateIndex(bound) = bound else {
            panic!("expected bound CREATE INDEX");
        };
        assert_eq!(bound.info.index_type, IndexType::HNSW);
        assert_eq!(bound.info.provider_config["m"], 32);
        assert_eq!(bound.info.provider_config["ef_construct"], 160);
        assert_eq!(bound.info.provider_config["dimension"], 100);
        assert_eq!(bound.info.provider_config["plain_scan_threshold"], 20_000);
        assert_eq!(
            bound.info.provider_config["inline_threshold"]["max_vector_count"],
            90_000
        );
    }

    #[test]
    fn bind_create_hnsw_index_rejects_unknown_or_invalid_options() {
        let vector_type = LogicalType::Array(Box::new(LogicalType::Float), 8);
        let mut binder = test_binder_with_public_table("items", &[("embedding", vector_type)]);

        let unknown = bind_create_index(
            &mut binder,
            parse_create_index_stmt(
                "CREATE VECTOR INDEX idx_unknown ON items (embedding) magic = 1",
            ),
        )
        .expect_err("unknown HNSW option should fail");
        assert!(unknown.to_string().contains("Unknown HNSW index option"));

        let invalid = bind_create_index(
            &mut binder,
            parse_create_index_stmt(
                "CREATE VECTOR INDEX idx_invalid ON items (embedding) m = 32 ef_construct = 16",
            ),
        )
        .expect_err("ef_construct below m should fail");
        assert!(invalid.to_string().contains("must be between m (32)"));
    }

    #[test]
    fn binder_rejects_create_aggregating_index_statement() {
        let mut binder = test_binder();
        let statement = parse_one("CREATE AGGREGATING INDEX idx_agg AS SELECT SUM(a) FROM t")
            .expect("statement should parse")
            .stmt;

        let err = binder
            .bind_statement_kind(statement)
            .expect_err("AGGREGATING INDEX should be rejected");

        assert!(
            err.to_string()
                .contains("AGGREGATING INDEX is not yet implemented"),
            "unexpected error: {err}"
        );
    }
}
