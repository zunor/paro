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
use paro_catalog::entry::{CatalogEntryEnum, CatalogType, StoredRoutineOverload};
use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;
use paro_function::scalar::cast::CastFunctionSet;
use paro_function::scalar::{
    ExpressionState, FunctionNullHandling, FunctionSideEffects, FunctionStability, ScalarBindInput,
    ScalarFunction,
};
use paro_parser::ast::{Expr, OrderByExpr};
use paro_routine::{
    BoundRoutineCallMeta, ExecutionBoundary, PlacementClass, RoutineCallIdentity, RoutineFamily,
    RoutineNullPolicy, RoutineReturn, RoutineSemantics, RoutineSideEffects, RoutineStability,
};

enum ResolvedScalarCallable {
    Native(std::sync::Arc<CatalogEntryEnum>),
    Routine(StoredRoutineOverload),
}

enum ResolvedAggregateCallable {
    Native(std::sync::Arc<CatalogEntryEnum>),
    Routine(StoredRoutineOverload),
}

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
    if distinct || filter.is_some() || !order_bys.is_empty() {
        return bind_aggregate_function(binder, schema, name, args, distinct, filter, order_bys);
    }

    let aggregate_args = args.clone();
    match bind_scalar_or_routine(binder, schema, name, args) {
        Ok(expression) => Ok(expression),
        Err(scalar_error) => match bind_aggregate_function(
            binder,
            schema,
            name,
            aggregate_args,
            distinct,
            filter,
            order_bys,
        ) {
            Ok(expression) => Ok(expression),
            Err(aggregate_error) => {
                if scalar_error.message().contains("not a scalar function")
                    || scalar_error.message().contains("not found")
                {
                    Err(aggregate_error)
                } else {
                    Err(scalar_error)
                }
            }
        },
    }
}

fn bind_scalar_or_routine(
    binder: &mut Binder,
    schema: Option<&str>,
    name: &str,
    args: Vec<Expr>,
) -> Result<Expression> {
    let (mut bound_args, arg_types) =
        bind_arguments(args, |arg| expr::bind_expression(binder, arg))?;

    match resolve_scalar_callable(binder, schema, name, &arg_types)? {
        ResolvedScalarCallable::Native(entry) => {
            let CatalogEntryEnum::ScalarFunction(func_entry) = &*entry else {
                let schema_name = schema.unwrap_or("public");
                return Err(paro_error::catalog(format!(
                    "{}.{} is not a scalar function",
                    schema_name, name
                )));
            };

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
        ResolvedScalarCallable::Routine(overload) => {
            bind_external_scalar_routine(name, overload, &arg_types, &mut bound_args, binder)
        }
    }
}

fn bind_aggregate_function(
    binder: &mut Binder,
    schema: Option<&str>,
    name: &str,
    args: Vec<Expr>,
    distinct: bool,
    filter: Option<Expr>,
    order_bys: Vec<OrderByExpr>,
) -> Result<Expression> {
    let cast_functions = binder.cast_functions.clone();
    let mut aggregate_binder = AggregateBinder::new(binder);

    let (mut bound_args, arg_types) = bind_arguments(args, |arg| aggregate_binder.bind(arg))?;

    match resolve_aggregate_callable(aggregate_binder.base.binder, schema, name, &arg_types)? {
        ResolvedAggregateCallable::Native(entry) => {
            let CatalogEntryEnum::AggregateFunction(agg_entry) = &*entry else {
                let schema_name = schema.unwrap_or("public");
                return Err(paro_error::catalog(format!(
                    "{}.{} is not an aggregate function",
                    schema_name, name
                )));
            };

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
        ResolvedAggregateCallable::Routine(overload) => match overload.spec.family {
            RoutineFamily::AggregateBatch => Err(paro_error::not_implemented(format!(
                "Aggregate routine '{}' binds through a dedicated external aggregate lowering path",
                name
            ))),
            RoutineFamily::WindowBatch => Err(paro_error::not_implemented(format!(
                "Window routine '{}' cannot bind as an aggregate",
                name
            ))),
            _ => Err(paro_error::syntax(format!(
                "Routine '{}' does not support aggregate syntax",
                name
            ))),
        },
    }
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

fn resolve_scalar_callable(
    binder: &Binder,
    schema: Option<&str>,
    name: &str,
    arg_types: &[LogicalType],
) -> Result<ResolvedScalarCallable> {
    let transaction = binder.catalog_txn_view();

    if let Some(schema_name) = schema {
        return resolve_scalar_callable_in_schema(
            binder,
            &transaction,
            schema_name,
            name,
            arg_types,
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
            if let Ok(entry) = resolve_scalar_callable_in_catalog(
                catalog.as_ref(),
                &transaction,
                &search_entry.schema,
                name,
                arg_types,
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

fn resolve_aggregate_callable(
    binder: &Binder,
    schema: Option<&str>,
    name: &str,
    arg_types: &[LogicalType],
) -> Result<ResolvedAggregateCallable> {
    let transaction = binder.catalog_txn_view();

    if let Some(schema_name) = schema {
        return resolve_aggregate_callable_in_schema(
            binder,
            &transaction,
            schema_name,
            name,
            arg_types,
        )
        .map_err(|_| {
            paro_error::catalog(format!("Function '{}.{}' not found", schema_name, name))
        });
    }

    for search_entry in binder.session_context().search_path() {
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
            if let Ok(entry) = resolve_aggregate_callable_in_catalog(
                catalog.as_ref(),
                &transaction,
                &search_entry.schema,
                name,
                arg_types,
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

fn resolve_scalar_callable_in_schema(
    binder: &Binder,
    transaction: &CatalogSnapshot,
    schema_name: &str,
    name: &str,
    arg_types: &[LogicalType],
) -> std::result::Result<ResolvedScalarCallable, ()> {
    resolve_scalar_callable_in_catalog(
        binder.catalog().as_ref(),
        transaction,
        schema_name,
        name,
        arg_types,
    )
}

fn resolve_scalar_callable_in_catalog(
    catalog: &ParoCatalog,
    transaction: &CatalogSnapshot,
    schema_name: &str,
    name: &str,
    arg_types: &[LogicalType],
) -> std::result::Result<ResolvedScalarCallable, ()> {
    if let Ok(entry) =
        catalog.get_any_entry(transaction, schema_name, CatalogType::ScalarFunction, name)
    {
        if let CatalogEntryEnum::ScalarFunction(func_entry) = &*entry {
            if func_entry.functions.bind(arg_types).is_ok() {
                return Ok(ResolvedScalarCallable::Native(entry));
            }
        }
    }

    if let Ok(entry) = catalog.get_any_entry(transaction, schema_name, CatalogType::Routine, name) {
        if let CatalogEntryEnum::Routine(routine_entry) = &*entry {
            if let Ok(overload) = routine_entry.resolve(arg_types) {
                return Ok(ResolvedScalarCallable::Routine(overload.clone()));
            }
        }
    }

    Err(())
}

fn resolve_aggregate_callable_in_schema(
    binder: &Binder,
    transaction: &CatalogSnapshot,
    schema_name: &str,
    name: &str,
    arg_types: &[LogicalType],
) -> std::result::Result<ResolvedAggregateCallable, ()> {
    resolve_aggregate_callable_in_catalog(
        binder.catalog().as_ref(),
        transaction,
        schema_name,
        name,
        arg_types,
    )
}

fn resolve_aggregate_callable_in_catalog(
    catalog: &ParoCatalog,
    transaction: &CatalogSnapshot,
    schema_name: &str,
    name: &str,
    arg_types: &[LogicalType],
) -> std::result::Result<ResolvedAggregateCallable, ()> {
    if let Ok(entry) = catalog.get_any_entry(
        transaction,
        schema_name,
        CatalogType::AggregateFunction,
        name,
    ) {
        if let CatalogEntryEnum::AggregateFunction(func_entry) = &*entry {
            if func_entry.functions.bind(arg_types).is_ok() {
                return Ok(ResolvedAggregateCallable::Native(entry));
            }
        }
    }

    if let Ok(entry) = catalog.get_any_entry(transaction, schema_name, CatalogType::Routine, name) {
        if let CatalogEntryEnum::Routine(routine_entry) = &*entry {
            if let Ok(overload) = routine_entry.resolve(arg_types) {
                return Ok(ResolvedAggregateCallable::Routine(overload.clone()));
            }
        }
    }

    Err(())
}

fn bind_external_scalar_routine(
    name: &str,
    overload: StoredRoutineOverload,
    arg_types: &[LogicalType],
    bound_args: &mut Vec<Expression>,
    binder: &Binder,
) -> Result<Expression> {
    let target_types = overload
        .spec
        .arguments
        .iter()
        .map(|arg| arg.data_type.clone())
        .collect::<Vec<_>>();
    apply_implicit_casts(
        bound_args,
        arg_types,
        &target_types,
        binder.cast_functions.as_ref(),
    )?;

    let return_type = match (&overload.spec.family, &overload.spec.return_type) {
        (RoutineFamily::ScalarBatch, RoutineReturn::Scalar(return_type)) => return_type.clone(),
        (RoutineFamily::TableBatch, RoutineReturn::Table(_)) => {
            return Err(paro_error::not_implemented(format!(
                "Table routine '{}' binds through FROM/LATERAL and late lowering, not scalar expression context",
                name
            )));
        }
        (RoutineFamily::AggregateBatch, _) => {
            return Err(paro_error::not_implemented(format!(
                "Aggregate routine '{}' binds through a dedicated external aggregate lowering path",
                name
            )));
        }
        (RoutineFamily::WindowBatch, _) => {
            return Err(paro_error::not_implemented(format!(
                "Window routine '{}' binds through a dedicated external window lowering path",
                name
            )));
        }
        _ => {
            return Err(paro_error::internal(format!(
                "routine '{}' has inconsistent family/return contract",
                name
            )));
        }
    };

    validate_non_literal_return_type(name, &return_type, false)?;
    let bound_function =
        external_scalar_placeholder(name, &target_types, &return_type, &overload.spec.semantics);
    let routine_meta = BoundRoutineCallMeta {
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
    };
    Ok(Expression::Function(
        FunctionExpression::new(bound_function, std::mem::take(bound_args), return_type)
            .with_routine_meta(routine_meta),
    ))
}

fn external_scalar_placeholder(
    name: &str,
    arguments: &[LogicalType],
    return_type: &LogicalType,
    semantics: &RoutineSemantics,
) -> paro_function::scalar::BoundScalarFunction {
    ScalarFunction::new(
        name.to_string(),
        arguments.to_vec(),
        return_type.clone(),
        external_scalar_placeholder_execute,
    )
    .with_stability(map_routine_stability(semantics.stability.clone()))
    .with_null_handling(map_routine_null_policy(semantics.null_policy.clone()))
    .with_side_effects(map_routine_side_effects(semantics.side_effects.clone()))
    .into()
}

fn external_scalar_placeholder_execute(
    _input: &Chunk,
    _state: &dyn ExpressionState,
    _result: &mut Vector,
) -> Result<()> {
    Err(paro_error::internal(
        "external routine reached native scalar executor before late lowering",
    ))
}

fn map_routine_stability(stability: RoutineStability) -> FunctionStability {
    match stability {
        RoutineStability::Immutable => FunctionStability::Consistent,
        RoutineStability::Stable => FunctionStability::ConsistentWithinQuery,
        RoutineStability::Volatile => FunctionStability::Volatile,
    }
}

fn map_routine_null_policy(policy: RoutineNullPolicy) -> FunctionNullHandling {
    match policy {
        RoutineNullPolicy::Strict => FunctionNullHandling::DefaultNullHandling,
        RoutineNullPolicy::CalledOnNullInput => FunctionNullHandling::SpecialHandling,
    }
}

fn map_routine_side_effects(side_effects: RoutineSideEffects) -> FunctionSideEffects {
    match side_effects {
        RoutineSideEffects::None => FunctionSideEffects::NoSideEffects,
        RoutineSideEffects::HasSideEffects => FunctionSideEffects::HasSideEffects,
    }
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
