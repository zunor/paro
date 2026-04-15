// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::binder::ir::{BoundBaseTable, BoundFromCTE, BoundFromItem, BoundFromSubquery};
use crate::binder::Binder;
use paro_catalog::entry::{CatalogObjectRef, CatalogType};
use paro_common::error::{self as paro_error, Result};
use paro_parser::ast::{Identifier, TableAlias};
use std::sync::Arc;

/// Bind a base table from a FROM clause.
///
///
///
/// ## Supported
/// - Table name resolution (1/2/3 part names)
/// - Catalog lookup
/// - Table alias with column aliases
/// - BindContext registration
/// - CTE reference ✅
///
/// ## Not Supported Yet
/// - Replacement scan (file-based)
/// - AT clause (time travel)
/// - Virtual columns
pub fn bind_base_table(
    binder: &mut Binder,
    database: Option<Identifier>,
    schema: Option<Identifier>,
    table: Identifier,
    alias: Option<TableAlias>,
) -> Result<BoundFromItem> {
    // 1. Check if this is a CTE reference (only for single-part names i.e. no database/schema prefix)
    let is_single_part_name = database.is_none() && schema.is_none();

    // 2. Resolve table, schema, and database names
    let (mut database_name, mut schema_name, mut table_name) = match (&database, &schema) {
        (Some(db), Some(sch)) => (db.name.clone(), sch.name.clone(), table.name.clone()),
        (None, Some(sch)) => (
            binder.catalog().name().to_string(),
            sch.name.clone(),
            table.name.clone(),
        ),
        (None, None) => (
            binder.catalog().name().to_string(),
            "public".to_string(),
            table.name.clone(),
        ),
        (Some(_), None) => {
            return Err(paro_error::catalog(
                "Invalid table reference: database provided without schema",
            ))
        }
    };

    // 2.1 Handle case-insensitivity for system schemas (unquoted)
    if database.as_ref().is_none_or(|id| id.quote.is_none())
        && database_name.eq_ignore_ascii_case(&binder.catalog().name())
    {
        database_name = binder.catalog().name().to_string();
    }

    if schema.as_ref().is_none_or(|id| id.quote.is_none()) {
        if schema_name.eq_ignore_ascii_case("information_schema") {
            schema_name = "information_schema".to_string();
        } else if schema_name.eq_ignore_ascii_case("pg_catalog") {
            schema_name = "pg_catalog".to_string();
        }
    }

    if table.quote.is_none() && (schema_name == "information_schema" || schema_name == "pg_catalog")
    {
        table_name = table_name.to_lowercase();
    }

    // 3. Check if this is a CTE reference (only for single-part names)
    if is_single_part_name {
        if let Some(cte_info) = binder.bind_context.get_cte(&table_name) {
            return bind_cte_ref(binder, cte_info, alias);
        }
    }

    // 3. Verify database (multi-tenancy verification)
    if database_name != binder.catalog().name() {
        return Err(paro_error::not_implemented(format!(
            "Cross-database table lookup ({})",
            database_name
        )));
    }

    // 4. Lookup table or view in catalog
    let table_or_view = if is_single_part_name {
        let search_path = binder.session_context().search_path();
        let mut found_entry = None;
        for search_entry in search_path {
            let catalog_name = if search_entry.catalog.is_empty() {
                binder.catalog().name().to_string()
            } else {
                search_entry.catalog.clone()
            };

            // Get the catalog for this entry
            let catalog = if catalog_name == binder.catalog().name() {
                Some(binder.catalog())
            } else {
                binder
                    .session_context()
                    .database(&catalog_name)
                    .map(|db| db.catalog.clone())
            };

            if let Some(catalog) = catalog {
                if let Ok(e) = catalog.get_table_or_view(
                    &binder.catalog_txn_view(),
                    &search_entry.schema,
                    &table_name,
                ) {
                    found_entry = Some(e);
                    break;
                }
            }
        }
        found_entry.ok_or_else(|| {
            paro_error::catalog(format!("Table or view '{}' not found", table_name))
        })?
    } else {
        binder
            .catalog()
            .get_table_or_view(&binder.catalog_txn_view(), &schema_name, &table_name)?
    };

    // 5. Handle table or view
    match table_or_view.as_ref() {
        paro_catalog::entry::CatalogEntryEnum::View(view_entry) => {
            // Found a view - expand it as a subquery
            bind_view_ref(binder, Arc::clone(view_entry), alias, &table_name)
        }
        paro_catalog::entry::CatalogEntryEnum::Table(table_entry) => {
            binder.record_regular_dependency(CatalogObjectRef::in_schema(
                table_entry.base.base.object_id,
                CatalogType::Table,
                table_entry.base.base.catalog.clone(),
                None,
                table_entry.base.schema_name.clone(),
                table_entry.base.base.name.clone(),
            ));
            // Continue with table binding below
            bind_table_entry(
                binder,
                Arc::clone(table_entry),
                alias,
                format!("{}.{}", schema_name, table_name),
                &table_name,
            )
        }
        _ => Err(paro_error::wrong_object_type("table or view", &table_name)),
    }
}

/// Bind a table entry (internal helper).
fn bind_table_entry(
    binder: &mut Binder,
    table_entry: std::sync::Arc<paro_catalog::entry::TableCatalogEntry>,
    alias: Option<TableAlias>,
    relation_name: String,
    table_name: &str,
) -> Result<BoundFromItem> {
    // Determine alias and column names
    let (table_alias, relation_alias, column_names, column_types) = if let Some(a) = alias {
        let alias_name = a.name.name.clone();
        let (names, types) = if a.columns.is_empty() {
            let names = table_entry
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>();
            let types = table_entry
                .columns
                .iter()
                .map(|c| c.logical_type.clone())
                .collect::<Vec<_>>();
            (names, types)
        } else {
            // If explicit column aliases are provided, use them
            if a.columns.len() != table_entry.columns.len() {
                return Err(paro_error::catalog(format!(
                    "Table alias column count mismatch for {}: expected {}, found {}",
                    alias_name,
                    table_entry.columns.len(),
                    a.columns.len()
                )));
            }
            let names = a.columns.iter().map(|c| c.name.clone()).collect();
            let types = table_entry
                .columns
                .iter()
                .map(|c| c.logical_type.clone())
                .collect();
            (names, types)
        };
        (alias_name.clone(), Some(alias_name), names, types)
    } else {
        let names = table_entry.columns.iter().map(|c| c.name.clone()).collect();
        let types = table_entry
            .columns
            .iter()
            .map(|c| c.logical_type.clone())
            .collect();
        (table_name.to_string(), None, names, types)
    };

    // 6. Generate table index and register in BindContext
    let table_index = binder.bind_context.generate_table_index();
    binder
        .bind_context
        .add_binding(table_alias, table_index, column_names, column_types);

    Ok(BoundFromItem::BaseTable(BoundBaseTable {
        table: table_entry,
        table_index,
        relation_name,
        relation_alias,
    }))
}

/// Bind a CTE reference.
fn bind_cte_ref(
    binder: &mut Binder,
    cte_info: Arc<crate::binder::ir::CTEBindState>,
    alias: Option<TableAlias>,
) -> Result<BoundFromItem> {
    let bound_cte = binder.get_or_bind_shared_cte(Arc::clone(&cte_info))?;

    // Determine alias and column names
    let (table_alias, column_names, column_types) = if let Some(a) = alias {
        let alias_name = a.name.name.clone();
        let (names, types) = if a.columns.is_empty() {
            (bound_cte.names.clone(), bound_cte.types.clone())
        } else {
            // If explicit column aliases are provided, use them
            if a.columns.len() != bound_cte.names.len() {
                return Err(paro_error::syntax(format!(
                    "CTE alias column count mismatch for {}: expected {}, found {}",
                    alias_name,
                    bound_cte.names.len(),
                    a.columns.len()
                )));
            }
            let names = a.columns.iter().map(|c| c.name.clone()).collect();
            (names, bound_cte.types.clone())
        };
        (alias_name, names, types)
    } else {
        (
            cte_info.info.name.clone(),
            bound_cte.names.clone(),
            bound_cte.types.clone(),
        )
    };

    // Generate table index and register in BindContext
    let table_index = binder.bind_context.generate_table_index();
    binder.bind_context.add_binding(
        table_alias.clone(),
        table_index,
        column_names.clone(),
        column_types.clone(),
    );

    Ok(BoundFromItem::CTE(BoundFromCTE {
        cte_index: bound_cte.cte_index,
        alias: table_alias,
        column_names,
        column_types,
        table_index,
    }))
}

/// Bind a view reference by expanding it as a subquery.
///
/// This function:
/// 1. Creates a child binder (VIEW_BINDER type) to bind the view's SELECT query
/// 2. Handles view column aliases (view aliases > query column names > user aliases)
/// 3. Returns a BoundFromSubquery (view is expanded as a subquery)
///
fn bind_view_ref(
    binder: &mut Binder,
    view_entry: std::sync::Arc<paro_catalog::entry::ViewCatalogEntry>,
    alias: Option<TableAlias>,
    original_table_name: &str,
) -> Result<BoundFromItem> {
    // 1. Create a child binder for the view (VIEW_BINDER type)
    // This ensures the view has its own BindContext and doesn't reference
    // CTEs defined in the outer query
    binder.record_regular_dependency(CatalogObjectRef::in_schema(
        view_entry.base.base.object_id,
        CatalogType::View,
        view_entry.base.base.catalog.clone(),
        None,
        view_entry.base.schema_name.clone(),
        view_entry.base.base.name.clone(),
    ));
    let mut view_binder = binder.create_child_without_dependency_collection();

    // 2. Clone the view's query for binding
    let view_query = (*view_entry.query).clone();

    // 3. Bind the view's SELECT query
    let mut bound_query = view_binder.bind_query(view_query)?;

    // 3.1 If view has explicit column types, add casts to the query
    let view_column_types = view_entry.get_column_types();
    if !view_column_types.is_empty() {
        let cast_functions = binder.session_context().cast_functions();
        bound_query.cast_to_types(view_column_types, &cast_functions)?;
    }

    // 4. Check for correlated columns (views should not have correlated references)
    if !view_binder.correlated_columns.is_empty() {
        return Err(paro_error::syntax(
            "Contents of view were altered - view bound correlated columns",
        ));
    }

    // 5. Construct view column names:
    //    Priority: (1) view aliases, (2) view query column names, (3) user-provided aliases
    let view_aliases = view_entry.get_aliases();
    let view_column_names = view_entry.get_column_names();
    let bound_names = bound_query.names();
    let bound_types = bound_query.types();

    // Start with view aliases, then fill in with query column names
    let mut final_names: Vec<String> = Vec::with_capacity(bound_names.len());
    for i in 0..bound_names.len() {
        if i < view_aliases.len() && !view_aliases[i].is_empty() {
            // Use view-defined alias
            final_names.push(view_aliases[i].clone());
        } else if i < view_column_names.len() && !view_column_names[i].is_empty() {
            // Use view's stored column name
            final_names.push(view_column_names[i].clone());
        } else {
            // Use bound query's column name
            final_names.push(bound_names[i].clone());
        }
    }

    // 6. Determine the subquery alias and apply user-provided column aliases
    let (subquery_alias, column_names, column_types) = if let Some(a) = alias {
        let alias_name = a.name.name.clone();
        let (names, types) = if a.columns.is_empty() {
            // No user column aliases - use the view's column names
            (final_names, bound_types)
        } else {
            // User provided column aliases - verify count matches
            if a.columns.len() != bound_names.len() {
                return Err(paro_error::syntax(format!(
                    "View alias '{}' specifies {} columns, but view returns {}",
                    alias_name,
                    a.columns.len(),
                    bound_names.len()
                )));
            }
            // Use user-provided column aliases
            let names = a.columns.iter().map(|c| c.name.clone()).collect();
            (names, bound_types)
        };
        (alias_name, names, types)
    } else {
        // No alias provided - use the original table name (view name)
        (original_table_name.to_string(), final_names, bound_types)
    };

    // 7. Generate subquery index and register in BindContext
    let subquery_index = binder.bind_context.generate_table_index();
    binder.bind_context.add_binding(
        subquery_alias.clone(),
        subquery_index,
        column_names.clone(),
        column_types.clone(),
    );

    // 8. Return the view as a subquery reference
    Ok(BoundFromItem::Subquery(BoundFromSubquery {
        subquery: Box::new(bound_query),
        alias: subquery_alias,
        column_names,
        column_types,
        subquery_index,
        lateral: false,
        correlated_columns: Vec::new(),
    }))
}
