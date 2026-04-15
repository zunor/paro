use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::binder::ir::BoundStatementKind;
use crate::binder::Binder;
use paro_catalog::entry::{
    CatalogEntryEnum, ConstraintType, CreatePropertyGraphInfo, EdgeTableInfo, TableCatalogEntry,
    VertexTableInfo,
};
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_parser::ast::{
    CreatePropertyGraphStmt, EdgeEndpointDef, Identifier, PropertyDef, PropertySpec,
};

#[derive(Debug, Clone)]
pub struct BoundCreatePropertyGraphInfo {
    pub info: CreatePropertyGraphInfo,
}

pub fn bind_create_property_graph(
    binder: &mut Binder,
    stmt: CreatePropertyGraphStmt,
) -> Result<BoundStatementKind> {
    let database_name = binder.catalog().name().to_string();
    let schema_name = binder.session_context().current_schema().to_string();
    let graph_name = stmt.graph_name.name.clone();
    let txn = binder.catalog_txn_view();
    let schema = binder.catalog().get_schema(&txn, &schema_name)?;

    // IF NOT EXISTS short-circuit: existing graph should not trigger further table/column checks.
    if schema.get_property_graph(&txn, &graph_name).is_ok() {
        if stmt.if_not_exists {
            let mut info = CreatePropertyGraphInfo::new(
                database_name.clone(),
                schema_name.clone(),
                graph_name,
            );
            info.if_not_exists = true;
            return Ok(BoundStatementKind::CreatePropertyGraph(
                BoundCreatePropertyGraphInfo { info },
            ));
        }
        return Err(paro_error::object_exists("property graph", &graph_name));
    }

    let mut label_set = HashSet::new();
    let mut vertex_table_lookup: HashMap<String, Arc<TableCatalogEntry>> = HashMap::new();
    let mut vertex_tables = Vec::with_capacity(stmt.vertex_tables.len());

    for vertex in &stmt.vertex_tables {
        let table_name = vertex.table_name.name.clone();
        if vertex_table_lookup.contains_key(&table_name) {
            return Err(paro_error::catalog(format!(
                "Duplicate vertex table \"{}\" in property graph \"{}\"",
                table_name, graph_name
            )));
        }

        let table = resolve_table_entry(
            binder,
            &schema_name,
            &table_name,
            &format!(
                "Vertex table \"{}\" in property graph \"{}\"",
                table_name, graph_name
            ),
        )?;
        let key_column_ids = resolve_key_column_ids(
            &table,
            vertex.key_columns.as_deref(),
            &format!("vertex table \"{}\"", table_name),
        )?;
        validate_supported_key_columns(
            &table,
            &key_column_ids,
            &format!("vertex table \"{}\" KEY", table_name),
        )?;
        let label = vertex
            .label
            .as_ref()
            .map(|ident| ident.name.clone())
            .unwrap_or_else(|| table_name.clone());
        ensure_unique_label(&mut label_set, &label, &graph_name)?;
        let property_column_ids = resolve_property_column_ids(
            &table,
            vertex.properties.as_ref(),
            &format!("vertex table \"{}\"", table_name),
        )?;

        vertex_tables.push(VertexTableInfo {
            table_name: table_name.clone(),
            table_oid: table.base.base.object_id.raw(),
            key_column_ids,
            label,
            property_column_ids,
        });
        vertex_table_lookup.insert(table_name, table);
    }

    let mut edge_tables = Vec::with_capacity(stmt.edge_tables.len());
    for edge in &stmt.edge_tables {
        let table_name = edge.table_name.name.clone();
        let table = resolve_table_entry(
            binder,
            &schema_name,
            &table_name,
            &format!(
                "Edge table \"{}\" in property graph \"{}\"",
                table_name, graph_name
            ),
        )?;

        // Edge table KEY is optional metadata; if not specified and no PRIMARY KEY exists,
        // use an empty key (only SOURCE KEY and DESTINATION KEY are required for graph construction).
        let key_column_ids = match edge.key_columns.as_deref() {
            Some(columns) => resolve_column_ids(
                &table,
                columns,
                &format!("edge table \"{}\" KEY columns", table_name),
            )?,
            None => {
                resolve_primary_key_column_ids(&table, &format!("edge table \"{}\"", table_name))
                    .unwrap_or_default()
            }
        };

        let (source_key_column_ids, source_vertex_table, source_ref_column_ids) =
            resolve_edge_endpoint(
                binder,
                &schema_name,
                &graph_name,
                &table,
                &edge.source,
                "SOURCE",
                &vertex_table_lookup,
            )?;
        let _source_key_type = validate_supported_key_columns(
            &table,
            &source_key_column_ids,
            &format!("edge table \"{}\" SOURCE KEY", table_name),
        )?;
        let (destination_key_column_ids, destination_vertex_table, destination_ref_column_ids) =
            resolve_edge_endpoint(
                binder,
                &schema_name,
                &graph_name,
                &table,
                &edge.destination,
                "DESTINATION",
                &vertex_table_lookup,
            )?;
        let _destination_key_type = validate_supported_key_columns(
            &table,
            &destination_key_column_ids,
            &format!("edge table \"{}\" DESTINATION KEY", table_name),
        )?;

        let label = edge
            .label
            .as_ref()
            .map(|ident| ident.name.clone())
            .unwrap_or_else(|| table_name.clone());
        ensure_unique_label(&mut label_set, &label, &graph_name)?;
        let property_column_ids = resolve_property_column_ids(
            &table,
            edge.properties.as_ref(),
            &format!("edge table \"{}\"", table_name),
        )?;

        edge_tables.push(EdgeTableInfo {
            table_name,
            table_oid: table.base.base.object_id.raw(),
            key_column_ids,
            source_key_column_ids,
            source_vertex_table,
            source_ref_column_ids,
            destination_key_column_ids,
            destination_vertex_table,
            destination_ref_column_ids,
            label,
            property_column_ids,
        });
    }

    let mut info = CreatePropertyGraphInfo::new(database_name, schema_name, graph_name);
    info.if_not_exists = stmt.if_not_exists;
    info.vertex_tables = vertex_tables;
    info.edge_tables = edge_tables;

    Ok(BoundStatementKind::CreatePropertyGraph(
        BoundCreatePropertyGraphInfo { info },
    ))
}

fn resolve_table_entry(
    binder: &Binder,
    schema_name: &str,
    table_name: &str,
    context: &str,
) -> Result<Arc<TableCatalogEntry>> {
    let entry = binder
        .catalog()
        .get_table(&binder.catalog_txn_view(), schema_name, table_name)
        .map_err(|_| paro_error::catalog(format!("{} does not exist", context)))?;

    match entry.as_ref() {
        CatalogEntryEnum::Table(table) => Ok(Arc::clone(table)),
        _ => Err(paro_error::wrong_object_type("table", table_name)),
    }
}

fn resolve_key_column_ids(
    table: &TableCatalogEntry,
    key_columns: Option<&[Identifier]>,
    context: &str,
) -> Result<Vec<u32>> {
    match key_columns {
        Some(columns) => resolve_column_ids(table, columns, &format!("{context} KEY columns")),
        None => resolve_primary_key_column_ids(table, context),
    }
}

fn resolve_primary_key_column_ids(table: &TableCatalogEntry, context: &str) -> Result<Vec<u32>> {
    let pk_constraint = table
        .constraints
        .iter()
        .find(|constraint| constraint.constraint_type == ConstraintType::PrimaryKey)
        .ok_or_else(|| {
            paro_error::catalog(format!(
                "{} must specify KEY columns because table \"{}\" has no PRIMARY KEY",
                context,
                table.name()
            ))
        })?;

    if pk_constraint.columns.is_empty() {
        return Err(paro_error::catalog(format!(
            "PRIMARY KEY on table \"{}\" is empty",
            table.name()
        )));
    }

    let mut seen = HashSet::new();
    let mut ids = Vec::with_capacity(pk_constraint.columns.len());
    for &column_idx in &pk_constraint.columns {
        if !seen.insert(column_idx) {
            return Err(paro_error::catalog(format!(
                "PRIMARY KEY on table \"{}\" contains duplicate column id {}",
                table.name(),
                column_idx
            )));
        }
        ids.push(to_u32_column_id(column_idx, table.name())?);
    }
    Ok(ids)
}

fn resolve_column_ids(
    table: &TableCatalogEntry,
    columns: &[Identifier],
    context: &str,
) -> Result<Vec<u32>> {
    let mut seen = HashSet::new();
    let mut ids = Vec::with_capacity(columns.len());
    for column in columns {
        let idx = table.get_column_index(&column.name).ok_or_else(|| {
            paro_error::catalog(format!(
                "{}: column \"{}\" does not exist on table \"{}\"",
                context,
                column.name,
                table.name()
            ))
        })?;
        if !seen.insert(idx) {
            return Err(paro_error::catalog(format!(
                "{}: duplicate column \"{}\"",
                context, column.name
            )));
        }
        ids.push(to_u32_column_id(idx, table.name())?);
    }
    Ok(ids)
}

fn resolve_property_column_ids(
    table: &TableCatalogEntry,
    property_spec: Option<&PropertySpec>,
    context: &str,
) -> Result<Vec<u32>> {
    match property_spec {
        None | Some(PropertySpec::All) => all_column_ids(table),
        Some(PropertySpec::None) => Ok(Vec::new()),
        Some(PropertySpec::Columns(columns)) => {
            resolve_property_columns_explicit(table, columns, context)
        }
        Some(PropertySpec::Except(excluded)) => {
            let mut excluded_set = HashSet::new();
            for column in excluded {
                let idx = table.get_column_index(&column.name).ok_or_else(|| {
                    paro_error::catalog(format!(
                        "{} PROPERTIES EXCEPT: column \"{}\" does not exist on table \"{}\"",
                        context,
                        column.name,
                        table.name()
                    ))
                })?;
                if !excluded_set.insert(idx) {
                    return Err(paro_error::catalog(format!(
                        "{} PROPERTIES EXCEPT contains duplicate column \"{}\"",
                        context, column.name
                    )));
                }
            }

            let mut ids = Vec::new();
            for idx in 0..table.columns.len() {
                if !excluded_set.contains(&idx) {
                    ids.push(to_u32_column_id(idx, table.name())?);
                }
            }
            Ok(ids)
        }
    }
}

fn resolve_property_columns_explicit(
    table: &TableCatalogEntry,
    properties: &[PropertyDef],
    context: &str,
) -> Result<Vec<u32>> {
    let mut seen = HashSet::new();
    let mut ids = Vec::with_capacity(properties.len());
    for property in properties {
        let idx = table
            .get_column_index(&property.column_name.name)
            .ok_or_else(|| {
                paro_error::catalog(format!(
                    "{} PROPERTIES: column \"{}\" does not exist on table \"{}\"",
                    context,
                    property.column_name.name,
                    table.name()
                ))
            })?;
        if !seen.insert(idx) {
            return Err(paro_error::catalog(format!(
                "{} PROPERTIES contains duplicate column \"{}\"",
                context, property.column_name.name
            )));
        }
        ids.push(to_u32_column_id(idx, table.name())?);
    }
    Ok(ids)
}

fn all_column_ids(table: &TableCatalogEntry) -> Result<Vec<u32>> {
    (0..table.columns.len())
        .map(|idx| to_u32_column_id(idx, table.name()))
        .collect()
}

fn resolve_edge_endpoint(
    binder: &Binder,
    schema_name: &str,
    graph_name: &str,
    edge_table: &TableCatalogEntry,
    endpoint: &EdgeEndpointDef,
    endpoint_kind: &str,
    vertex_tables: &HashMap<String, Arc<TableCatalogEntry>>,
) -> Result<(Vec<u32>, String, Vec<u32>)> {
    let key_column_ids = resolve_column_ids(
        edge_table,
        &endpoint.key_columns,
        &format!(
            "{endpoint_kind} KEY columns of edge table \"{}\"",
            edge_table.name()
        ),
    )?;

    let ref_table_name = endpoint.references_table.name.clone();
    let ref_table = resolve_table_entry(
        binder,
        schema_name,
        &ref_table_name,
        &format!(
            "{endpoint_kind} REFERENCES table \"{}\" in property graph \"{}\"",
            ref_table_name, graph_name
        ),
    )?;

    if !vertex_tables.contains_key(&ref_table_name) {
        return Err(paro_error::catalog(format!(
            "{} REFERENCES table \"{}\" must be listed in VERTEX TABLES for property graph \"{}\"",
            endpoint_kind, ref_table_name, graph_name
        )));
    }

    let ref_column_ids = match endpoint.references_columns.as_deref() {
        Some(columns) => resolve_column_ids(
            &ref_table,
            columns,
            &format!(
                "{endpoint_kind} REFERENCES columns on table \"{}\"",
                ref_table_name
            ),
        )?,
        None => resolve_primary_key_column_ids(
            &ref_table,
            &format!("{endpoint_kind} REFERENCES table \"{}\"", ref_table_name),
        )?,
    };

    if key_column_ids.len() != ref_column_ids.len() {
        return Err(paro_error::catalog(format!(
            "{} KEY column count ({}) must match REFERENCES column count ({}) on edge table \"{}\"",
            endpoint_kind,
            key_column_ids.len(),
            ref_column_ids.len(),
            edge_table.name()
        )));
    }

    let edge_key_type = validate_supported_key_columns(
        edge_table,
        &key_column_ids,
        &format!(
            "{endpoint_kind} KEY on edge table \"{}\"",
            edge_table.name()
        ),
    )?;
    let ref_key_type = validate_supported_key_columns(
        &ref_table,
        &ref_column_ids,
        &format!("{endpoint_kind} REFERENCES table \"{}\"", ref_table_name),
    )?;
    if edge_key_type != ref_key_type {
        return Err(paro_error::catalog(format!(
            "{} KEY on edge table \"{}\" has type ({}) but REFERENCES table \"{}\" uses ({})",
            endpoint_kind,
            edge_table.name(),
            format_key_signature(&edge_key_type),
            ref_table_name,
            format_key_signature(&ref_key_type)
        )));
    }

    Ok((key_column_ids, ref_table_name, ref_column_ids))
}

fn ensure_unique_label(labels: &mut HashSet<String>, label: &str, graph_name: &str) -> Result<()> {
    if labels.insert(label.to_string()) {
        Ok(())
    } else {
        Err(paro_error::catalog(format!(
            "Duplicate label \"{}\" in property graph \"{}\"",
            label, graph_name
        )))
    }
}

fn to_u32_column_id(idx: usize, table_name: &str) -> Result<u32> {
    u32::try_from(idx).map_err(|_| {
        paro_error::catalog(format!(
            "Column index {} in table \"{}\" exceeds u32 range",
            idx, table_name
        ))
    })
}

fn validate_supported_key_columns(
    table: &TableCatalogEntry,
    column_ids: &[u32],
    context: &str,
) -> Result<Vec<LogicalType>> {
    if column_ids.is_empty() {
        return Err(paro_error::catalog(format!(
            "{} must use at least one BIGINT or VARCHAR column",
            context
        )));
    }

    let mut signature = Vec::with_capacity(column_ids.len());
    for &column_id in column_ids {
        let column_idx = usize::try_from(column_id).map_err(|_| {
            paro_error::catalog(format!(
                "{} column id {} exceeds usize range",
                context, column_id
            ))
        })?;
        let column = table.columns.get(column_idx).ok_or_else(|| {
            paro_error::catalog(format!(
                "{} column id {} does not exist on table \"{}\"",
                context,
                column_id,
                table.name()
            ))
        })?;

        if column.logical_type != LogicalType::BigInt && column.logical_type != LogicalType::Varchar
        {
            return Err(paro_error::catalog(format!(
                "{} must use only BIGINT or VARCHAR columns, found {} on table \"{}\"",
                context,
                column.logical_type,
                table.name()
            )));
        }
        signature.push(column.logical_type.clone());
    }

    Ok(signature)
}

fn format_key_signature(signature: &[LogicalType]) -> String {
    signature
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}
