//! Column Expression Binder
use crate::binder::Binder;
use crate::expression::{ColumnRefExpression, Expression};
use crate::operator::ColumnBinding;
use paro_common::error::{self as paro_error, Result};
use paro_parser::ast::ColumnRef;

/// Bind a column reference from ColumnRef.
///
/// Handles schema-qualified column references (e.g., `schema.table.column`).
/// When a schema is specified, the table must exist in that schema.
pub fn bind_column_ref_from_column_ref(
    binder: &mut Binder,
    column_ref: ColumnRef,
) -> Result<Expression> {
    // Extract schema, table, and column names from ColumnRef
    let schema_name = column_ref.schema.map(|s| s.name);
    let table_name = column_ref.table.map(|t| t.name);
    let column_name = column_ref.column.name().to_string();

    bind_column_ref_inner(binder, schema_name, table_name, column_name)
}

/// Internal function to bind column reference with schema, table, and column name.
///
/// # Schema Handling
/// When a schema is specified (e.g., `pg_catalog.pg_class.oid`):
/// - The schema is used for validation but not for binding lookup
/// - Current implementation: schema is noted but bindings are matched by table alias only
/// - Future: could validate that the table actually belongs to the specified schema
///
/// # Lookup Order
/// 1. Search in current BindContext bindings
/// 2. If not found, search outer [`BindContext`] scopes (correlated columns)
fn bind_column_ref_inner(
    binder: &mut Binder,
    schema_name: Option<String>,
    table_name: Option<String>,
    column_name: String,
) -> Result<Expression> {
    // Note: schema_name is currently used for error messages only.
    // In the future, we could validate that the table belongs to the specified schema.
    // For now, we match by table alias (which is typically the table name without schema).

    if let Some(local) = binder
        .bind_context
        .lookup_local_column(table_name.as_deref(), &column_name)?
    {
        let table_index = local.table_index;
        let column_index = local.column_index;
        let return_type = local.return_type;
        let binding = ColumnBinding::new(table_index, column_index);
        Ok(Expression::ColumnRef(ColumnRefExpression::new(
            binding,
            return_type,
        )))
    } else {
        // 3. Resolve virtual rowid pseudo-column.
        if column_name.eq_ignore_ascii_case("rowid") {
            let mut rowid_binding = None;
            for binding in binder.bind_context.iter_bindings() {
                if let Some(ref t_name) = table_name {
                    if t_name != &binding.alias {
                        continue;
                    }
                }

                if rowid_binding.is_some() {
                    return Err(paro_error::catalog(format!(
                        "Ambiguous column name: {}",
                        column_name
                    )));
                }
                rowid_binding = Some((binding.index, binding.column_names.len()));
            }

            if let Some((table_index, rowid_col_idx)) = rowid_binding {
                binder.mark_row_id_binding(table_index);
                let binding = ColumnBinding::new(table_index, rowid_col_idx);
                return Ok(Expression::ColumnRef(ColumnRefExpression::new(
                    binding,
                    paro_common::types::LogicalType::BigInt,
                )));
            }
        }

        // 3. Correlated columns: outer scopes only (same as former parent-binder walk), via
        // BindContext.parent. Depth matches the old Binder.parent chain (first outer = 1).
        if let Some(corr) = binder
            .bind_context
            .lookup_outer_column(table_name.as_deref(), &column_name)?
        {
            let corr = crate::binder::CorrelatedColumnInfo {
                table_index: corr.table_index,
                column_index: corr.column_index,
                return_type: corr.return_type.clone(),
                name: column_name.clone(),
                depth: corr.depth,
            };
            binder.correlated_columns.push(corr.clone());
            let binding = ColumnBinding::new(corr.table_index, corr.column_index);
            return Ok(Expression::ColumnRef(ColumnRefExpression::with_depth(
                binding,
                corr.return_type,
                corr.depth,
            )));
        }

        // Build a descriptive error message including schema if present
        let full_column_ref = match (&schema_name, &table_name) {
            (Some(s), Some(t)) => format!("{}.{}.{}", s, t, column_name),
            (None, Some(t)) => format!("{}.{}", t, column_name),
            _ => column_name.clone(),
        };

        Err(paro_error::catalog(format!(
            "Column not found: {}",
            full_column_ref
        )))
    }
}
