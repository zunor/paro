// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::binder::bind::clause::AggregateBinder;
use crate::binder::bind::expr;
use crate::binder::Binder;
use crate::expression::{
    AggregateExpression, AggregateType, CastExpression, Expression, FunctionExpression,
    OrderByExpression,
};
use paro_catalog::database_catalog::ParoCatalog;
use paro_catalog::entry::{CatalogEntryEnum, CatalogType};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_function::scalar::cast::CastFunctionSet;
use paro_function::scalar::ScalarBindInput;
use paro_parser::ast::{Expr, OrderByExpr};

/// Bind a function call expression.
///
/// # Arguments
/// * `binder` - The binder context
/// * `schema` - Optional schema name (e.g., "pg_catalog" in `pg_catalog.pg_get_userbyid()`)
/// * `name` - Function name
/// * `args` - Function arguments as AST expressions
/// * `distinct` - Whether the aggregate uses DISTINCT
/// * `filter` - Optional aggregate FILTER clause
/// * `order_bys` - Optional WITHIN GROUP / ordered aggregate expressions
///
/// # Schema Resolution
/// - If `schema` is specified, only search in that schema
/// - If `schema` is None, search in the default search path: pg_catalog, then public
pub fn bind_function(
    binder: &mut Binder,
    schema: Option<&str>,
    name: &str,
    args: Vec<Expr>,
    distinct: bool,
    filter: Option<Expr>,
    order_bys: Vec<OrderByExpr>,
) -> Result<Expression> {
    let entry = lookup_function_entry(
        binder,
        schema,
        name,
        distinct || filter.is_some() || !order_bys.is_empty(),
    )?;

    match &*entry {
        CatalogEntryEnum::ScalarFunction(func_entry) => {
            reject_non_aggregate_modifiers(name, distinct, filter.as_ref(), &order_bys)?;

            let (mut bound_args, arg_types) =
                bind_arguments(args, |arg| expr::bind_expression(binder, arg))?;

            let (scalar_func, target_types) = func_entry.functions.bind(&arg_types)?;
            validate_non_literal_target_types(name, &target_types, false)?;
            apply_implicit_casts(
                &mut bound_args,
                &arg_types,
                &target_types,
                binder.cast_functions.as_ref(),
            )?;

            let bind_input =
                ScalarBindInput::new(target_types.clone(), collect_constant_values(&bound_args));
            let bound_function = scalar_func.bind(&bind_input)?;
            let return_type = bound_function.return_type.clone();
            validate_non_literal_return_type(name, &return_type, false)?;

            Ok(Expression::Function(FunctionExpression::new(
                bound_function,
                bound_args,
                return_type,
            )))
        }
        CatalogEntryEnum::AggregateFunction(agg_entry) => {
            let cast_functions = binder.cast_functions.clone();
            let mut aggregate_binder = AggregateBinder::new(binder);

            let (mut bound_args, arg_types) =
                bind_arguments(args, |arg| aggregate_binder.bind(arg))?;

            let (agg_func, target_types) = agg_entry.functions.bind(&arg_types)?;
            validate_non_literal_target_types(name, &target_types, true)?;
            apply_implicit_casts(
                &mut bound_args,
                &arg_types,
                &target_types,
                cast_functions.as_ref(),
            )?;

            let bound_filter = match filter {
                Some(filter_expr) => Some(CastExpression::add_cast_if_needed(
                    aggregate_binder.bind(filter_expr)?,
                    LogicalType::Boolean,
                    cast_functions.as_ref(),
                )?),
                None => None,
            };
            let bound_order_bys = bind_order_bys(order_bys, |expr| aggregate_binder.bind(expr))?;

            let return_type = agg_func.return_type.clone();
            validate_non_literal_return_type(name, &return_type, true)?;

            let bind_info = agg_func.bind_data.clone();
            Ok(Expression::Aggregate(
                AggregateExpression::new(agg_func, bound_args, return_type)
                    .with_aggr_type(if distinct {
                        AggregateType::Distinct
                    } else {
                        AggregateType::NonDistinct
                    })
                    .with_filter(bound_filter)
                    .with_order_bys(bound_order_bys)
                    .with_bind_info(bind_info),
            ))
        }
        _ => {
            let schema_name = schema.unwrap_or("public");
            Err(paro_error::catalog(format!(
                "{}.{} is not a function",
                schema_name, name
            )))
        }
    }
}

fn reject_non_aggregate_modifiers(
    name: &str,
    distinct: bool,
    filter: Option<&Expr>,
    order_bys: &[OrderByExpr],
) -> Result<()> {
    if distinct {
        return Err(paro_error::syntax(format!(
            "DISTINCT is only supported for aggregate functions: {}",
            name
        )));
    }
    if filter.is_some() {
        return Err(paro_error::syntax(format!(
            "FILTER is only supported for aggregate functions: {}",
            name
        )));
    }
    if !order_bys.is_empty() {
        return Err(paro_error::syntax(format!(
            "WITHIN GROUP ORDER BY is only supported for aggregate functions: {}",
            name
        )));
    }
    Ok(())
}

fn bind_arguments<F>(
    args: Vec<Expr>,
    mut binder_fn: F,
) -> Result<(Vec<Expression>, Vec<LogicalType>)>
where
    F: FnMut(Expr) -> Result<Expression>,
{
    let mut bound_args = Vec::with_capacity(args.len());
    let mut arg_types = Vec::with_capacity(args.len());

    for arg in args {
        let bound = binder_fn(arg)?;
        arg_types.push(bound.get_expression_return_type());
        bound_args.push(bound);
    }

    Ok((bound_args, arg_types))
}

fn bind_order_bys<F>(
    order_bys: Vec<OrderByExpr>,
    mut binder_fn: F,
) -> Result<Vec<OrderByExpression>>
where
    F: FnMut(Expr) -> Result<Expression>,
{
    let mut result = Vec::with_capacity(order_bys.len());
    for order in order_bys {
        let expression = binder_fn(order.expr)?;
        let ascending = order.asc.unwrap_or(true);
        let nulls_first = order.nulls_first.unwrap_or(!ascending);
        result.push(OrderByExpression {
            expression,
            ascending,
            nulls_first,
        });
    }
    Ok(result)
}

fn apply_implicit_casts(
    bound_args: &mut [Expression],
    arg_types: &[LogicalType],
    target_types: &[LogicalType],
    cast_functions: &CastFunctionSet,
) -> Result<()> {
    for (i, (arg, target_type)) in bound_args.iter_mut().zip(target_types.iter()).enumerate() {
        if arg_types[i] != *target_type {
            *arg = CastExpression::add_cast_if_needed(
                arg.clone(),
                target_type.clone(),
                cast_functions,
            )?;
        }
    }
    Ok(())
}

fn validate_non_literal_target_types(
    name: &str,
    target_types: &[LogicalType],
    aggregate: bool,
) -> Result<()> {
    for target_type in target_types {
        if matches!(
            target_type,
            LogicalType::IntegerLiteral(_) | LogicalType::StringLiteral
        ) {
            let kind = if aggregate {
                "Aggregate function"
            } else {
                "Function"
            };
            return Err(paro_error::internal(format!(
                "{} '{}' returned a literal type ({}) - return an explicit type instead",
                kind, name, target_type
            )));
        }
    }
    Ok(())
}

fn collect_constant_values(arguments: &[Expression]) -> Vec<Option<Value>> {
    arguments.iter().map(try_extract_constant_value).collect()
}

fn try_extract_constant_value(expr: &Expression) -> Option<Value> {
    match expr {
        Expression::Constant(constant) => Some(constant.value.clone()),
        Expression::Cast(cast) => {
            let value = try_extract_constant_value(&cast.child)?;
            value.cast(&cast.target_type).ok()
        }
        _ => None,
    }
}

fn validate_non_literal_return_type(
    name: &str,
    return_type: &LogicalType,
    aggregate: bool,
) -> Result<()> {
    if matches!(
        return_type,
        LogicalType::IntegerLiteral(_) | LogicalType::StringLiteral
    ) {
        let kind = if aggregate {
            "Aggregate function"
        } else {
            "Function"
        };
        return Err(paro_error::internal(format!(
            "{} '{}' returned a literal type ({}) - return an explicit type instead",
            kind, name, return_type
        )));
    }
    Ok(())
}

/// Look up a function entry in the catalog with schema resolution.
///
/// # Schema Resolution Strategy
/// - If `schema` is specified, only search in that schema
/// - If `schema` is None, search in the default search path (pg_catalog, public)
fn lookup_function_entry(
    binder: &Binder,
    schema: Option<&str>,
    name: &str,
    prefers_aggregate: bool,
) -> Result<std::sync::Arc<CatalogEntryEnum>> {
    let transaction = binder.catalog_txn_view();

    if let Some(schema_name) = schema {
        return lookup_function_entry_in_schema(
            binder,
            &transaction,
            schema_name,
            name,
            prefers_aggregate,
        )
        .map_err(|_| {
            paro_error::catalog(format!("Function '{}.{}' not found", schema_name, name))
        });
    }

    // No schema specified, search in session search path
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

        if let Some(catalog) = catalog {
            if let Ok(entry) = lookup_catalog_function_entry(
                catalog.as_ref(),
                &transaction,
                &search_entry.schema,
                name,
                prefers_aggregate,
            ) {
                return Ok(entry);
            }
        }
    }

    Err(paro_error::catalog(format!(
        "Function '{}' not found in search path",
        name
    )))
}

fn lookup_function_entry_in_schema(
    binder: &Binder,
    transaction: &CatalogSnapshot,
    schema_name: &str,
    name: &str,
    prefers_aggregate: bool,
) -> std::result::Result<std::sync::Arc<CatalogEntryEnum>, ()> {
    lookup_catalog_function_entry(
        binder.catalog().as_ref(),
        transaction,
        schema_name,
        name,
        prefers_aggregate,
    )
}

fn lookup_catalog_function_entry(
    catalog: &ParoCatalog,
    transaction: &CatalogSnapshot,
    schema_name: &str,
    name: &str,
    prefers_aggregate: bool,
) -> std::result::Result<std::sync::Arc<CatalogEntryEnum>, ()> {
    let search_order = if prefers_aggregate {
        [CatalogType::AggregateFunction, CatalogType::ScalarFunction]
    } else {
        [CatalogType::ScalarFunction, CatalogType::AggregateFunction]
    };

    for catalog_type in search_order {
        if let Ok(entry) = catalog.get_any_entry(transaction, schema_name, catalog_type, name) {
            return Ok(entry);
        }
    }

    Err(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::test_utils::test_binder_with_search_path;
    use paro_catalog::collection::InstallMode;
    use paro_catalog::entry::{
        AggregateFunctionCatalogEntry, CatalogEntryEnum, CatalogType, ScalarFunctionCatalogEntry,
    };
    use paro_catalog::search_path::CatalogSearchEntry;
    use paro_function::aggregate::distributive::{
        count::get_count_function, sum::get_sum_function,
    };
    use paro_function::scalar::string::get_substring_functions;
    use paro_parser::ast::{BinaryOperator, Expr, Literal, OrderByExpr};
    use std::sync::Arc;

    fn uint_literal(value: u64) -> Expr {
        Expr::Literal {
            span: None,
            value: Literal::UInt64(value),
        }
    }

    fn string_literal(value: &str) -> Expr {
        Expr::Literal {
            span: None,
            value: Literal::String(value.to_string()),
        }
    }

    fn equals_expr(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            span: None,
            op: BinaryOperator::Eq,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    fn bind_aggregate(
        schema: Option<&str>,
        name: &str,
        args: Vec<Expr>,
        distinct: bool,
        filter: Option<Expr>,
        order_bys: Vec<OrderByExpr>,
    ) -> AggregateExpression {
        let mut binder = test_binder_with_search_path(vec![
            CatalogSearchEntry::schema_only("pg_catalog"),
            CatalogSearchEntry::schema_only("public"),
        ]);
        install_test_aggregate(&binder, "pg_catalog", get_count_function());
        install_test_aggregate(&binder, "pg_catalog", get_sum_function());
        let expr = bind_function(&mut binder, schema, name, args, distinct, filter, order_bys)
            .expect("bind aggregate");
        let Expression::Aggregate(aggregate) = expr else {
            panic!("expected bound aggregate expression");
        };
        aggregate
    }

    fn install_test_aggregate(
        binder: &Binder,
        schema_name: &str,
        set: paro_function::aggregate::AggregateFunctionSet,
    ) {
        let txn = binder.catalog_txn_view();
        let schema = binder
            .catalog()
            .get_schema(&txn, schema_name)
            .expect("schema should exist in test catalog");
        let entry = Arc::new(AggregateFunctionCatalogEntry::new(
            schema.base.catalog.clone(),
            schema.base.name.clone(),
            set,
            0,
        ));
        let _ = schema
            .collection(CatalogType::AggregateFunction)
            .expect("aggregate function collection")
            .install_committed(
                Arc::new(CatalogEntryEnum::AggregateFunction(entry)),
                InstallMode::RejectExisting,
            );
    }

    fn install_test_scalar(
        binder: &Binder,
        schema_name: &str,
        set: paro_function::scalar::ScalarFunctionSet,
    ) {
        let txn = binder.catalog_txn_view();
        let schema = binder
            .catalog()
            .get_schema(&txn, schema_name)
            .expect("schema should exist in test catalog");
        let entry = Arc::new(ScalarFunctionCatalogEntry::new(
            schema.base.catalog.clone(),
            schema.base.name.clone(),
            set,
            0,
        ));
        let _ = schema
            .collection(CatalogType::ScalarFunction)
            .expect("scalar function collection")
            .install_committed(
                Arc::new(CatalogEntryEnum::ScalarFunction(entry)),
                InstallMode::RejectExisting,
            );
    }

    #[test]
    fn count_distinct_binds_as_distinct_aggregate() {
        let aggregate = bind_aggregate(None, "count", vec![uint_literal(1)], true, None, vec![]);
        assert_eq!(aggregate.aggr_type, AggregateType::Distinct);
        assert!(aggregate.filter.is_none());
        assert!(aggregate.order_bys.is_empty());
    }

    #[test]
    fn aggregate_filter_is_bound_and_preserved() {
        let aggregate = bind_aggregate(
            None,
            "sum",
            vec![uint_literal(1)],
            false,
            Some(equals_expr(uint_literal(1), uint_literal(1))),
            vec![],
        );
        assert_eq!(aggregate.aggr_type, AggregateType::NonDistinct);
        assert!(aggregate.filter.is_some());
        assert!(aggregate.order_bys.is_empty());
    }

    #[test]
    fn ordered_aggregate_binds_order_list() {
        let aggregate = bind_aggregate(
            None,
            "sum",
            vec![uint_literal(1)],
            false,
            None,
            vec![OrderByExpr {
                expr: uint_literal(1),
                asc: Some(false),
                nulls_first: Some(false),
            }],
        );
        assert_eq!(aggregate.aggr_type, AggregateType::NonDistinct);
        assert!(aggregate.filter.is_none());
        assert_eq!(aggregate.order_bys.len(), 1);
        assert!(!aggregate.order_bys[0].ascending);
        assert!(!aggregate.order_bys[0].nulls_first);
    }

    #[test]
    fn aggregate_lookup_supports_explicit_pg_catalog_schema() {
        let aggregate = bind_aggregate(
            Some("pg_catalog"),
            "sum",
            vec![uint_literal(1)],
            false,
            None,
            vec![],
        );
        assert_eq!(aggregate.aggr_type, AggregateType::NonDistinct);
    }

    #[test]
    fn scalar_bind_attaches_bound_data_after_implicit_casts() {
        let mut binder = test_binder_with_search_path(vec![
            CatalogSearchEntry::schema_only("pg_catalog"),
            CatalogSearchEntry::schema_only("public"),
        ]);
        install_test_scalar(&binder, "pg_catalog", get_substring_functions());

        let expr = bind_function(
            &mut binder,
            None,
            "substring",
            vec![string_literal("hello"), uint_literal(2), uint_literal(3)],
            false,
            None,
            vec![],
        )
        .expect("bind substring");

        let Expression::Function(function) = expr else {
            panic!("expected function expression");
        };
        assert!(function.function.bind_data.is_some());
        assert_eq!(function.function.name, "substring");
        assert_eq!(function.return_type, LogicalType::Varchar);
    }
}
