// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Binds table functions in `FROM` (`generate_series`, aliases, named args, `WITH ORDINALITY`, table in/out).
//! Non-constant arguments still require constant folding before bind.

use crate::binder::bind::expr;
use crate::binder::ir::{BoundExternalRoutine, BoundFromItem, BoundTableFunction};
use crate::binder::plan::subquery::{
    split_child_correlated_columns, CorrelationBoundaryMode, CorrelationProjectionMode,
};
use crate::binder::Binder;
use crate::expression::*;
use paro_catalog::entry::{CatalogEntryEnum, CatalogType, StoredRoutineOverload};
use paro_common::error::{self as paro_error, Result};
use paro_function::table::{TableFunction, TableFunctionBindInput};
use paro_parser::ast::{Expr, Identifier, TableAlias};
use paro_routine::{
    BoundRoutineCallMeta, ExecutionBoundary, PlacementClass, RoutineCallIdentity, RoutineFamily,
    RoutineReturn,
};
use std::collections::HashMap;
use std::mem;
use std::sync::Arc;

/// Bind a table function reference.
///
/// This handles the `function(args)` pattern in FROM clauses.
/// Example: `SELECT * FROM generate_series(1, 10)`
/// Example with named params: `SELECT * FROM repeat_row(1, 'hello', num_rows => 3)`
/// Example with ordinality: `SELECT * FROM generate_series(1, 10) WITH ORDINALITY`
pub fn bind_table_function_ref(
    binder: &mut Binder,
    function_name: &str,
    args: Vec<Expr>,
    named_params: Vec<(Identifier, Expr)>,
    alias: Option<TableAlias>,
    lateral: bool,
    with_ordinality: bool,
) -> Result<BoundFromItem> {
    // 1. Look up the native table function entry before enforcing native-only
    // constant-argument semantics. External table routines can accept bound
    // expressions and correlated columns.
    let Some(entry) = lookup_catalog_entry(binder, CatalogType::TableFunction, function_name)?
    else {
        return bind_external_table_routine_ref(
            binder,
            function_name,
            args,
            alias,
            lateral,
            with_ordinality,
        );
    };

    // 2. Bind the function arguments first (recursive binding of expressions)
    let (bound_args, arg_values, arg_types) = bind_function_arguments(binder, &args)?;

    // 3. Bind named parameters
    let (bound_named_args, named_param_values) = bind_named_parameters(binder, &named_params)?;

    let tf_entry = entry.as_table_function().ok_or_else(|| {
        paro_error::catalog(format!("'{}' is not a table function", function_name))
    })?;

    // 4. Find the best matching function overload
    let (matched_function, target_types) = tf_entry.functions.bind_with_types(&arg_types)?;
    let matched_function = matched_function.clone();

    // 5. Add implicit casts to arguments if needed
    let mut final_bound_arguments = Vec::new();
    for (i, (arg, target_type)) in bound_args.into_iter().zip(target_types.iter()).enumerate() {
        if arg_types[i] != *target_type {
            final_bound_arguments.push(CastExpression::add_cast_if_needed(
                arg,
                target_type.clone(),
                &binder.cast_functions,
            )?);
        } else {
            final_bound_arguments.push(arg);
        }
    }

    // 6. Validate named parameters against function definition and add casts
    // Note: bound_named_args already has the bound expressions, named_param_values has values.
    // We need to iterate through named_params to match identifiers with target types.
    let mut final_bound_named_arguments = Vec::new();
    for i in 0..named_params.len() {
        let (name_ident, _) = &named_params[i];
        let name = name_ident.name.to_lowercase();
        let bound_expr = bound_named_args[i].clone();
        let value = &named_param_values[&name];

        // Find named parameter definition
        let param_def = matched_function
            .named_parameters
            .iter()
            .find(|(n, _)| n.to_lowercase() == name);
        if let Some((_, target_type)) = param_def {
            if value.logical_type() != *target_type {
                let casted_expr = CastExpression::add_cast_if_needed(
                    bound_expr,
                    target_type.clone(),
                    &binder.cast_functions,
                )?;
                final_bound_named_arguments.push(casted_expr);
            } else {
                final_bound_named_arguments.push(bound_expr);
            }
        } else {
            final_bound_named_arguments.push(bound_expr);
        }
    }
    validate_named_parameters(&matched_function, &named_param_values)?;

    // 7. Call the bind function to get column info
    let (return_types, return_names) =
        call_bind_function(&matched_function, &arg_values, &named_param_values)?;

    // 7. Add ordinality column if WITH ORDINALITY is specified
    let (final_types, final_names) = if with_ordinality {
        let mut types = return_types.clone();
        let mut names = return_names.clone();
        types.push(paro_common::types::LogicalType::BigInt);
        names.push("ordinality".to_string());
        (types, names)
    } else {
        (return_types, return_names)
    };

    // 8. Determine alias and column names
    let (table_alias, column_names, column_types) =
        determine_alias_and_columns(function_name, &final_names, &final_types, alias)?;

    // 9. Register the table function in BindContext
    let table_index = binder.bind_context.generate_table_index();
    binder.bind_context.add_binding(
        table_alias.clone(),
        table_index,
        column_names.clone(),
        column_types.clone(),
    );

    // 10. Combine bound arguments
    let mut all_bound_args = final_bound_arguments;
    all_bound_args.extend(final_bound_named_arguments);

    // 11. Create the bound table function ref
    let is_in_out = matched_function.is_in_out_function();

    Ok(BoundFromItem::TableFunction(BoundTableFunction {
        function: Arc::new(matched_function),
        alias: table_alias,
        column_names,
        column_types,
        table_index,
        bound_arguments: all_bound_args,
        input_table_types: Vec::new(),
        input_table_names: Vec::new(),
        is_in_out_function: is_in_out,
        child_table: None,
        with_ordinality,
    }))
}

fn lookup_catalog_entry(
    binder: &Binder,
    catalog_type: CatalogType,
    name: &str,
) -> Result<Option<Arc<CatalogEntryEnum>>> {
    let search_path = binder.session_context().search_path();
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

        let Some(catalog) = catalog else {
            continue;
        };
        let Ok(schema) = catalog.get_schema(&binder.catalog_txn_view(), &search_entry.schema)
        else {
            continue;
        };
        let Some(entry) = schema
            .collection(catalog_type)
            .expect("catalog collection")
            .get_entry(
                binder.catalog_txn_view().transaction_id,
                binder.catalog_txn_view().start_time,
                name,
            )
        else {
            continue;
        };
        return Ok(Some(entry));
    }

    Ok(None)
}

fn bind_external_table_routine_ref(
    binder: &mut Binder,
    function_name: &str,
    args: Vec<Expr>,
    alias: Option<TableAlias>,
    lateral: bool,
    with_ordinality: bool,
) -> Result<BoundFromItem> {
    if with_ordinality {
        return Err(paro_error::not_implemented(
            "WITH ORDINALITY for external table routines",
        ));
    }

    let Some(entry) = lookup_catalog_entry(binder, CatalogType::Routine, function_name)? else {
        return Err(paro_error::catalog(format!(
            "Table function '{}' not found",
            function_name
        )));
    };

    let CatalogEntryEnum::Routine(routine_entry) = &*entry else {
        return Err(paro_error::catalog(format!(
            "'{}' is not a routine",
            function_name
        )));
    };

    let mut child_binder = binder.create_child();
    let mut bound_arguments = Vec::with_capacity(args.len());
    let mut argument_types = Vec::with_capacity(args.len());
    for arg in args {
        let bound = expr::bind_expression(&mut child_binder, arg)?;
        argument_types.push(bound.return_type());
        bound_arguments.push(bound);
    }

    let overload = routine_entry.resolve(&argument_types)?;
    let split = split_child_correlated_columns(
        mem::take(&mut child_binder.correlated_columns),
        CorrelationBoundaryMode::ScopeBoundary,
    );
    let correlated_columns = if lateral {
        split.projected_correlations(CorrelationProjectionMode::IncludeDepthOnePropagated)
    } else {
        split.local_to_child_parent.clone()
    };
    binder.correlated_columns.extend(split.propagate_to_parent);

    if !lateral && !correlated_columns.is_empty() {
        return Err(paro_error::syntax(
            "Table routine in FROM cannot reference outer columns without LATERAL",
        ));
    }

    let target_types = overload
        .spec
        .arguments
        .iter()
        .map(|arg| arg.data_type.clone())
        .collect::<Vec<_>>();
    let mut final_arguments = Vec::with_capacity(bound_arguments.len());
    for (index, arg) in bound_arguments.into_iter().enumerate() {
        if argument_types[index] != target_types[index] {
            final_arguments.push(CastExpression::add_cast_if_needed(
                arg,
                target_types[index].clone(),
                &binder.cast_functions,
            )?);
        } else {
            final_arguments.push(arg);
        }
    }

    let (column_names, column_types) = match (&overload.spec.family, &overload.spec.return_type) {
        (RoutineFamily::TableBatch, RoutineReturn::Table(columns)) => (
            columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>(),
            columns
                .iter()
                .map(|column| column.data_type.clone())
                .collect::<Vec<_>>(),
        ),
        (RoutineFamily::ScalarBatch, _) => {
            return Err(paro_error::not_implemented(format!(
                "Scalar routine '{}' cannot bind through FROM",
                function_name
            )));
        }
        (RoutineFamily::AggregateBatch, _) => {
            return Err(paro_error::not_implemented(format!(
                "Aggregate routine '{}' binds through a dedicated external aggregate path",
                function_name
            )));
        }
        (RoutineFamily::WindowBatch, _) => {
            return Err(paro_error::not_implemented(format!(
                "Window routine '{}' binds through a dedicated external window path",
                function_name
            )));
        }
        _ => {
            return Err(paro_error::internal(format!(
                "routine '{}' has inconsistent family/return contract",
                function_name
            )));
        }
    };

    let (table_alias, final_names, final_types) =
        determine_alias_and_columns(function_name, &column_names, &column_types, alias)?;
    let table_index = binder.bind_context.generate_table_index();
    binder.bind_context.add_binding(
        table_alias.clone(),
        table_index,
        final_names.clone(),
        final_types.clone(),
    );

    let routine_meta = routine_meta_from_overload(&overload);
    let call_expression = Expression::Function(
        FunctionExpression::new(
            external_table_placeholder(function_name, &target_types, &column_types),
            final_arguments.clone(),
            final_types
                .first()
                .cloned()
                .unwrap_or(paro_common::types::LogicalType::Unknown),
        )
        .with_routine_meta(routine_meta.clone()),
    );

    Ok(BoundFromItem::ExternalRoutine(BoundExternalRoutine {
        alias: table_alias,
        column_names: final_names,
        column_types: final_types,
        table_index,
        call_expression,
        bound_arguments: final_arguments,
        call: routine_meta,
        lateral,
        correlated_columns,
    }))
}

fn routine_meta_from_overload(overload: &StoredRoutineOverload) -> BoundRoutineCallMeta {
    BoundRoutineCallMeta {
        identity: RoutineCallIdentity::Catalog {
            routine_id: overload.spec.identity.id,
            generation: overload.spec.identity.generation,
        },
        semantics: overload.spec.semantics.clone(),
        boundary: ExecutionBoundary {
            placement: PlacementClass::External,
            may_block: overload.spec.semantics.may_block,
            row_semantics: overload.spec.semantics.row_semantics.clone(),
        },
        spec: Some(overload.spec.clone()),
    }
}

fn external_table_placeholder(
    name: &str,
    arguments: &[paro_common::types::LogicalType],
    return_columns: &[paro_common::types::LogicalType],
) -> paro_function::scalar::BoundScalarFunction {
    let return_type = return_columns
        .first()
        .cloned()
        .unwrap_or(paro_common::types::LogicalType::Unknown);
    paro_function::scalar::ScalarFunction::new(
        name.to_string(),
        arguments.to_vec(),
        return_type,
        |_, _, _| {
            Err(paro_error::internal(
                "external table routine reached native scalar executor before physical lowering",
            ))
        },
    )
    .into()
}

/// Bind function arguments.
fn bind_function_arguments(
    binder: &mut Binder,
    args: &[Expr],
) -> Result<(
    Vec<Expression>,
    Vec<paro_common::runtime_value::Value>,
    Vec<paro_common::types::LogicalType>,
)> {
    let mut bound_args = Vec::new();
    let mut arg_values = Vec::new();
    let mut arg_types = Vec::new();

    for arg in args {
        let bound = expr::bind_expression(binder, arg.clone())?;

        // Extract value if it's a constant
        if let Expression::Constant(ref const_expr) = bound {
            arg_values.push(const_expr.value.clone());
        } else {
            return Err(paro_error::not_implemented(
                "Non-constant arguments to table functions",
            ));
        }

        arg_types.push(bound.return_type());
        bound_args.push(bound);
    }

    Ok((bound_args, arg_values, arg_types))
}

/// Bind named parameters.
///
/// Named parameters are specified as `name => value` in the function call.
/// Example: `repeat_row(1, 'hello', num_rows => 3)`
fn bind_named_parameters(
    binder: &mut Binder,
    named_params: &[(Identifier, Expr)],
) -> Result<(
    Vec<Expression>,
    HashMap<String, paro_common::runtime_value::Value>,
)> {
    let mut bound_args = Vec::new();
    let mut named_values = HashMap::new();

    for (name, expr) in named_params {
        let bound = expr::bind_expression(binder, expr.clone())?;

        // Extract value if it's a constant
        if let Expression::Constant(ref const_expr) = bound {
            let param_name = name.name.to_lowercase();

            // Check for duplicate named parameters
            if named_values.contains_key(&param_name) {
                return Err(paro_error::syntax(format!(
                    "Duplicate named parameter: {}",
                    param_name
                )));
            }

            named_values.insert(param_name, const_expr.value.clone());
        } else {
            return Err(paro_error::not_implemented(
                "Non-constant named parameters to table functions",
            ));
        }

        bound_args.push(bound);
    }

    Ok((bound_args, named_values))
}

/// Validate named parameters against function definition.
///
/// Checks that:
/// 1. All provided named parameters are defined in the function
/// 2. Named parameter types are compatible
fn validate_named_parameters(
    function: &TableFunction,
    named_params: &HashMap<String, paro_common::runtime_value::Value>,
) -> Result<()> {
    for (param_name, param_value) in named_params {
        // Find the parameter in the function definition
        let param_def = function
            .named_parameters
            .iter()
            .find(|(name, _)| name.to_lowercase() == *param_name);

        match param_def {
            Some((_, expected_type)) => {
                // Check type compatibility
                let actual_type = param_value.logical_type();
                let cast_cost = paro_common::cast_rules::CastRules::implicit_cast_cost(
                    &actual_type,
                    expected_type,
                );
                if cast_cost < 0 {
                    return Err(paro_error::syntax(format!(
                        "Named parameter '{}' has type {}, expected {}",
                        param_name, actual_type, expected_type
                    )));
                }
            }
            None => {
                return Err(paro_error::syntax(format!(
                    "Unknown named parameter '{}' for table function '{}'",
                    param_name, function.name
                )));
            }
        }
    }

    Ok(())
}

/// Call the bind function to get return types and names.
fn call_bind_function(
    function: &TableFunction,
    arg_values: &[paro_common::runtime_value::Value],
    named_params: &HashMap<String, paro_common::runtime_value::Value>,
) -> Result<(Vec<paro_common::types::LogicalType>, Vec<String>)> {
    call_bind_function_with_input_table(function, arg_values, named_params, &[], &[])
}

/// Call the bind function with input table types (for table-in-out functions).
fn call_bind_function_with_input_table(
    function: &TableFunction,
    arg_values: &[paro_common::runtime_value::Value],
    named_params: &HashMap<String, paro_common::runtime_value::Value>,
    input_table_types: &[paro_common::types::LogicalType],
    input_table_names: &[String],
) -> Result<(Vec<paro_common::types::LogicalType>, Vec<String>)> {
    let bind_fn = function.bind.ok_or_else(|| {
        paro_error::internal(format!(
            "Table function '{}' has no bind function",
            function.name
        ))
    })?;

    let input = TableFunctionBindInput {
        inputs: arg_values,
        named_parameters: named_params,
        input_table_types,
        input_table_names,
    };

    let mut return_types = Vec::new();
    let mut return_names = Vec::new();

    let _bind_data = bind_fn(&input, &mut return_types, &mut return_names)?;

    if return_types.is_empty() {
        return Err(paro_error::syntax(format!(
            "Table function '{}' must return at least one column",
            function.name
        )));
    }

    Ok((return_types, return_names))
}

/// Determine the alias and column names for a table function.
fn determine_alias_and_columns(
    function_name: &str,
    return_names: &[String],
    return_types: &[paro_common::types::LogicalType],
    alias: Option<TableAlias>,
) -> Result<(String, Vec<String>, Vec<paro_common::types::LogicalType>)> {
    let column_types = return_types.to_vec();

    if let Some(table_alias) = alias {
        let alias_name = table_alias.name.name.clone();

        // If explicit column aliases are provided
        let column_names = if table_alias.columns.is_empty() {
            // Use the original column names from the function
            return_names.to_vec()
        } else {
            // Verify column count matches
            if table_alias.columns.len() != return_names.len() {
                return Err(paro_error::syntax(format!(
                    "Table function alias '{}' specifies {} columns, but function returns {}",
                    alias_name,
                    table_alias.columns.len(),
                    return_names.len()
                )));
            }
            // Use the provided column aliases
            table_alias.columns.iter().map(|c| c.name.clone()).collect()
        };

        Ok((alias_name, column_names, column_types))
    } else {
        // Use function name as alias
        Ok((
            function_name.to_string(),
            return_names.to_vec(),
            column_types,
        ))
    }
}

/// Bind a table-in-out function reference.
///
/// Table-in-out functions process input data from a child table reference.
/// Example: `SELECT * FROM t, unnest(t.array_column)`
///
/// # Arguments
/// * `binder` - The binder context
/// * `function_name` - Name of the table function
/// * `args` - Function arguments (may include column references)
/// * `named_params` - Named parameters
/// * `alias` - Optional table alias
/// * `child_table` - The input table reference
/// * `with_ordinality` - Whether WITH ORDINALITY is specified
///
/// # Returns
/// A bound table function reference with `is_in_out_function = true`
pub fn bind_table_in_out_function_ref(
    binder: &mut Binder,
    function_name: &str,
    args: Vec<Expr>,
    named_params: Vec<(Identifier, Expr)>,
    alias: Option<TableAlias>,
    child_table: BoundFromItem,
    with_ordinality: bool,
) -> Result<BoundFromItem> {
    // 1. Get input table types and names from child
    let (input_table_types, input_table_names) = get_input_table_info(&child_table);

    // 2. Bind the function arguments
    let (bound_args, arg_values, arg_types) = bind_function_arguments(binder, &args)?;

    // 3. Bind named parameters
    let (bound_named_args, named_param_values) = bind_named_parameters(binder, &named_params)?;

    // 4. Look up the table function in the catalog
    let search_path = binder.session_context().search_path();
    let mut entry = None;

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
            if let Ok(schema) = catalog.get_schema(&binder.catalog_txn_view(), &search_entry.schema)
            {
                if let Some(e) = schema
                    .collection(CatalogType::TableFunction)
                    .expect("table function collection")
                    .get_entry(
                        binder.catalog_txn_view().transaction_id,
                        binder.catalog_txn_view().start_time,
                        function_name,
                    )
                {
                    entry = Some(e);
                    break;
                }
            }
        }
    }

    let entry = entry.ok_or_else(|| {
        paro_error::catalog(format!("Table function '{}' not found", function_name))
    })?;

    let tf_entry = entry.as_table_function().ok_or_else(|| {
        paro_error::catalog(format!("'{}' is not a table function", function_name))
    })?;

    // 5. Find the best matching function overload
    let matched_function = tf_entry.functions.bind(&arg_types)?.clone();

    // 6. Verify this is a table-in-out function
    if !matched_function.is_in_out_function() {
        return Err(paro_error::syntax(format!(
            "Function '{}' is not a table-in-out function",
            function_name
        )));
    }

    // 7. Validate named parameters
    validate_named_parameters(&matched_function, &named_param_values)?;

    // 8. Call the bind function with input table info
    let (return_types, return_names) = call_bind_function_with_input_table(
        &matched_function,
        &arg_values,
        &named_param_values,
        &input_table_types,
        &input_table_names,
    )?;

    // 9. Add ordinality column if WITH ORDINALITY is specified
    let (final_types, final_names) = if with_ordinality {
        let mut types = return_types.clone();
        let mut names = return_names.clone();
        types.push(paro_common::types::LogicalType::BigInt);
        names.push("ordinality".to_string());
        (types, names)
    } else {
        (return_types, return_names)
    };

    // 10. Determine alias and column names
    let (table_alias, column_names, column_types) =
        determine_alias_and_columns(function_name, &final_names, &final_types, alias)?;

    // 11. Register the table function in BindContext
    let table_index = binder.bind_context.generate_table_index();
    binder.bind_context.add_binding(
        table_alias.clone(),
        table_index,
        column_names.clone(),
        column_types.clone(),
    );

    // 12. Combine bound arguments
    let mut all_bound_args = bound_args;
    all_bound_args.extend(bound_named_args);

    // 13. Create the bound table function ref
    Ok(BoundFromItem::TableFunction(BoundTableFunction {
        function: Arc::new(matched_function),
        alias: table_alias,
        column_names,
        column_types,
        table_index,
        bound_arguments: all_bound_args,
        input_table_types,
        input_table_names,
        is_in_out_function: true,
        child_table: Some(Box::new(child_table)),
        with_ordinality,
    }))
}

/// Extract input table types and names from a bound table reference.
fn get_input_table_info(
    table_ref: &BoundFromItem,
) -> (Vec<paro_common::types::LogicalType>, Vec<String>) {
    match table_ref {
        BoundFromItem::BaseTable(base) => {
            let types = base
                .table
                .columns
                .iter()
                .map(|c| c.logical_type.clone())
                .collect();
            let names = base.table.columns.iter().map(|c| c.name.clone()).collect();
            (types, names)
        }
        BoundFromItem::Subquery(subquery) => {
            (subquery.column_types.clone(), subquery.column_names.clone())
        }
        BoundFromItem::TableFunction(tf) => (tf.column_types.clone(), tf.column_names.clone()),
        BoundFromItem::ExternalRoutine(routine) => {
            (routine.column_types.clone(), routine.column_names.clone())
        }
        BoundFromItem::Join(join) => {
            // For joins, combine columns from both sides
            let (left_types, left_names) = get_input_table_info(&join.left);
            let (right_types, right_names) = get_input_table_info(&join.right);
            let mut types = left_types;
            types.extend(right_types);
            let mut names = left_names;
            names.extend(right_names);
            (types, names)
        }
        BoundFromItem::CTE(cte) => (cte.column_types.clone(), cte.column_names.clone()),
        BoundFromItem::GraphTable(graph) => {
            (graph.output_types.clone(), graph.output_names.clone())
        }
    }
}
