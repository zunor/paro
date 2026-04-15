// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_function::scalar::cast::date_casts::{
    parse_date_text, parse_interval_text, parse_time_text, parse_timestamp_text,
    parse_timestamptz_text,
};
use paro_parser::ast::{Expr, Statement};
use paro_parser::{parse_expr_tokens, tokenize_sql, StatementReplacer, StatementVisitor};

use crate::prepared::portal::bind_value_types;
use crate::prepared::typed_parameters::{BoundParameter, TypedParameterEnv};

pub(crate) fn placeholder_count(stmt: &Statement) -> usize {
    let mut count = 0usize;
    let mut visitor = StatementVisitor::new(
        |expr| {
            if matches!(expr, Expr::Placeholder { .. }) {
                count = count.saturating_add(1);
            }
        },
        |_| {},
    );
    visitor.visit(stmt);
    count
}

pub(crate) fn bind_value_arguments(
    stmt: &Statement,
    values: &[Value],
    parameter_types: &[Option<LogicalType>],
) -> Result<Statement> {
    let resolved = resolve_parameters(parameter_types, values)?;
    let exprs = resolved
        .iter()
        .map(|param| render_parameter_expr(&param.value, Some(&param.logical_type)))
        .collect::<Result<Vec<_>>>()?;
    bind_expr_arguments(stmt, &exprs)
}

pub(crate) fn bind_expr_arguments(stmt: &Statement, exprs: &[Expr]) -> Result<Statement> {
    let mut stmt = stmt.clone();
    let mut next = 0usize;
    let mut placeholders = 0usize;
    let mut first_error = None;

    let mut replacer = StatementReplacer::new(
        |expr| {
            if !matches!(expr, Expr::Placeholder { .. }) {
                return;
            }

            placeholders = placeholders.saturating_add(1);
            match exprs.get(next) {
                Some(replacement) => {
                    *expr = replacement.clone();
                    next = next.saturating_add(1);
                }
                None if first_error.is_none() => {
                    first_error = Some(paro_error::syntax(format!(
                        "expected {placeholders} parameters, got {}",
                        exprs.len()
                    )));
                }
                None => {}
            }
        },
        |_| {},
    );
    replacer.visit(&mut stmt);

    if let Some(err) = first_error {
        return Err(err);
    }
    if next != exprs.len() {
        return Err(paro_error::syntax(format!(
            "expected {placeholders} parameters, got {}",
            exprs.len()
        )));
    }
    Ok(stmt)
}

pub(crate) fn render_probe_statement(
    stmt: &Statement,
    parameter_types: &[Option<LogicalType>],
) -> Result<Option<Statement>> {
    if parameter_types.is_empty() {
        return Ok(None);
    }

    let exprs = parameter_types
        .iter()
        .map(|ty| {
            render_parameter_expr(
                &Value::Null(ty.clone().unwrap_or(LogicalType::Unknown)),
                ty.as_ref(),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    bind_expr_arguments(stmt, &exprs).map(Some)
}

pub(crate) fn typed_null_parameter_env(
    parameter_types: &[Option<LogicalType>],
) -> TypedParameterEnv {
    TypedParameterEnv::new(
        parameter_types
            .iter()
            .map(|ty| BoundParameter::null(ty.clone().unwrap_or(LogicalType::Unknown)))
            .collect(),
    )
}

pub(crate) fn typed_parameter_env_from_values(
    parameter_types: &[Option<LogicalType>],
    values: &[Value],
) -> Result<TypedParameterEnv> {
    Ok(TypedParameterEnv::new(resolve_parameters(
        parameter_types,
        values,
    )?))
}

pub(crate) fn parse_text_parameter_value(
    bytes: Option<&[u8]>,
    expected_type: Option<&LogicalType>,
) -> Result<Value> {
    let Some(bytes) = bytes else {
        return Ok(Value::Null(
            expected_type.cloned().unwrap_or(LogicalType::Unknown),
        ));
    };

    let text = std::str::from_utf8(bytes).map_err(|_| {
        paro_error::protocol_violation("extended query parameter must be valid UTF-8".to_string())
    })?;

    match expected_type {
        Some(LogicalType::Boolean) => parse_bool(text).map(Value::Boolean),
        Some(LogicalType::TinyInt) => parse_i64(text, "tinyint").and_then(|v| {
            i8::try_from(v)
                .map(Value::TinyInt)
                .map_err(|_| paro_error::invalid_value("tinyint", text))
        }),
        Some(LogicalType::SmallInt) => parse_i64(text, "smallint").and_then(|v| {
            i16::try_from(v)
                .map(Value::SmallInt)
                .map_err(|_| paro_error::invalid_value("smallint", text))
        }),
        Some(LogicalType::Integer) => parse_i64(text, "integer").and_then(|v| {
            i32::try_from(v)
                .map(Value::Integer)
                .map_err(|_| paro_error::invalid_value("integer", text))
        }),
        Some(LogicalType::BigInt) => parse_i64(text, "bigint").map(Value::BigInt),
        Some(LogicalType::Float) => parse_f64(text, "real").map(|v| Value::Float(v as f32)),
        Some(LogicalType::Double) => parse_f64(text, "double precision").map(Value::Double),
        Some(LogicalType::Varchar)
        | Some(LogicalType::VarcharCollation(_))
        | Some(LogicalType::Json)
        | Some(LogicalType::Jsonb)
        | Some(LogicalType::TsVector)
        | Some(LogicalType::TsQuery)
        | Some(LogicalType::Date)
        | Some(LogicalType::Time)
        | Some(LogicalType::Timestamp)
        | Some(LogicalType::TimestampTz)
        | Some(LogicalType::Interval)
        | Some(LogicalType::Decimal { .. })
        | Some(LogicalType::Array(_, _))
        | Some(LogicalType::List(_))
        | Some(LogicalType::Struct(_))
        | Some(LogicalType::HugeInt)
        | Some(LogicalType::UTinyInt)
        | Some(LogicalType::USmallInt)
        | Some(LogicalType::UInteger)
        | Some(LogicalType::UBigInt)
        | Some(LogicalType::UHugeInt)
        | Some(LogicalType::Blob)
        | Some(LogicalType::Uuid)
        | Some(LogicalType::Null)
        | Some(LogicalType::IntegerLiteral(_))
        | Some(LogicalType::StringLiteral)
        | Some(LogicalType::Unknown) => Ok(Value::Varchar(text.to_string())),
        None => Ok(infer_text_parameter_value(text)),
    }
}

fn resolve_parameters(
    parameter_types: &[Option<LogicalType>],
    values: &[Value],
) -> Result<Vec<BoundParameter>> {
    if parameter_types.len() != values.len() {
        return Err(paro_error::syntax(format!(
            "expected {} parameters, got {}",
            parameter_types.len(),
            values.len()
        )));
    }

    let inferred = bind_value_types(values);
    parameter_types
        .iter()
        .zip(values)
        .zip(inferred)
        .map(|((declared, value), inferred)| {
            let Some(logical_type) = declared.clone().or(inferred) else {
                return Err(paro_error::syntax(
                    "could not infer parameter type".to_string(),
                ));
            };
            let value = cast_parameter_value(value, &logical_type)?;
            let resolved_type = resolved_parameter_logical_type(&value, &logical_type);
            Ok(BoundParameter::new(value, resolved_type))
        })
        .collect()
}

fn cast_parameter_value(value: &Value, logical_type: &LogicalType) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::Null(logical_type.clone()));
    }

    if is_string_backed_parameter_type(logical_type) && matches!(value, Value::Varchar(_)) {
        return Ok(value.clone());
    }

    if &value.logical_type() == logical_type {
        return Ok(value.clone());
    }

    match (value, logical_type) {
        (Value::BigInt(v), LogicalType::SmallInt) => i16::try_from(*v)
            .map(Value::SmallInt)
            .map_err(|_| paro_error::invalid_value("smallint", value.to_string())),
        (Value::BigInt(v), LogicalType::Integer) => i32::try_from(*v)
            .map(Value::Integer)
            .map_err(|_| paro_error::invalid_value("integer", value.to_string())),
        (Value::BigInt(v), LogicalType::TinyInt) => i8::try_from(*v)
            .map(Value::TinyInt)
            .map_err(|_| paro_error::invalid_value("tinyint", value.to_string())),
        (Value::BigInt(v), LogicalType::UTinyInt) => u8::try_from(*v)
            .map(Value::UTinyInt)
            .map_err(|_| paro_error::invalid_value("utinyint", value.to_string())),
        (Value::BigInt(v), LogicalType::USmallInt) => u16::try_from(*v)
            .map(Value::USmallInt)
            .map_err(|_| paro_error::invalid_value("usmallint", value.to_string())),
        (Value::BigInt(v), LogicalType::UInteger) => u32::try_from(*v)
            .map(Value::UInteger)
            .map_err(|_| paro_error::invalid_value("uinteger", value.to_string())),
        (Value::BigInt(v), LogicalType::HugeInt) => Ok(Value::HugeInt(i128::from(*v))),
        (Value::BigInt(v), LogicalType::UBigInt) => u64::try_from(*v)
            .map(Value::UBigInt)
            .map_err(|_| paro_error::invalid_value("ubigint", value.to_string())),
        (Value::BigInt(v), LogicalType::UHugeInt) => u128::try_from(*v)
            .map(Value::UHugeInt)
            .map_err(|_| paro_error::invalid_value("uhugeint", value.to_string())),
        (Value::BigInt(v), LogicalType::Float) => Ok(Value::Float(*v as f32)),
        (Value::BigInt(v), LogicalType::Double) => Ok(Value::Double(*v as f64)),
        (Value::Integer(v), LogicalType::SmallInt) => i16::try_from(*v)
            .map(Value::SmallInt)
            .map_err(|_| paro_error::invalid_value("smallint", value.to_string())),
        (Value::Integer(v), LogicalType::TinyInt) => i8::try_from(*v)
            .map(Value::TinyInt)
            .map_err(|_| paro_error::invalid_value("tinyint", value.to_string())),
        (Value::Integer(v), LogicalType::UTinyInt) => u8::try_from(*v)
            .map(Value::UTinyInt)
            .map_err(|_| paro_error::invalid_value("utinyint", value.to_string())),
        (Value::Integer(v), LogicalType::USmallInt) => u16::try_from(*v)
            .map(Value::USmallInt)
            .map_err(|_| paro_error::invalid_value("usmallint", value.to_string())),
        (Value::Integer(v), LogicalType::HugeInt) => Ok(Value::HugeInt(i128::from(*v))),
        (Value::UInteger(v), LogicalType::BigInt) => Ok(Value::BigInt(i64::from(*v))),
        (Value::USmallInt(v), LogicalType::Integer) => Ok(Value::Integer(i32::from(*v))),
        (Value::UTinyInt(v), LogicalType::SmallInt) => Ok(Value::SmallInt(i16::from(*v))),
        (Value::Float(v), LogicalType::Double) => Ok(Value::Double(f64::from(*v))),
        (Value::Varchar(text), LogicalType::TinyInt) => parse_exact_integer(text, "tinyint")
            .and_then(|v| {
                i8::try_from(v)
                    .map(Value::TinyInt)
                    .map_err(|_| paro_error::invalid_value("tinyint", text))
            }),
        (Value::Varchar(text), LogicalType::UTinyInt) => parse_exact_unsigned(text, "utinyint")
            .and_then(|v| {
                u8::try_from(v)
                    .map(Value::UTinyInt)
                    .map_err(|_| paro_error::invalid_value("utinyint", text))
            }),
        (Value::Varchar(text), LogicalType::SmallInt) => parse_exact_integer(text, "smallint")
            .and_then(|v| {
                i16::try_from(v)
                    .map(Value::SmallInt)
                    .map_err(|_| paro_error::invalid_value("smallint", text))
            }),
        (Value::Varchar(text), LogicalType::USmallInt) => parse_exact_unsigned(text, "usmallint")
            .and_then(|v| {
                u16::try_from(v)
                    .map(Value::USmallInt)
                    .map_err(|_| paro_error::invalid_value("usmallint", text))
            }),
        (Value::Varchar(text), LogicalType::Integer) => parse_exact_integer(text, "integer")
            .and_then(|v| {
                i32::try_from(v)
                    .map(Value::Integer)
                    .map_err(|_| paro_error::invalid_value("integer", text))
            }),
        (Value::Varchar(text), LogicalType::UInteger) => parse_exact_unsigned(text, "uinteger")
            .and_then(|v| {
                u32::try_from(v)
                    .map(Value::UInteger)
                    .map_err(|_| paro_error::invalid_value("uinteger", text))
            }),
        (Value::Varchar(text), LogicalType::BigInt) => {
            parse_exact_integer(text, "bigint").map(Value::BigInt)
        }
        (Value::Varchar(text), LogicalType::HugeInt) => parse_hugeint(text).map(Value::HugeInt),
        (Value::Varchar(text), LogicalType::UBigInt) => {
            parse_exact_unsigned(text, "ubigint").map(Value::UBigInt)
        }
        (Value::Varchar(text), LogicalType::UHugeInt) => parse_uhugeint(text).map(Value::UHugeInt),
        (Value::Varchar(text), LogicalType::Float) => {
            parse_f64(text, "real").map(|v| Value::Float(v as f32))
        }
        (Value::Varchar(text), LogicalType::Double) => {
            parse_f64(text, "double precision").map(Value::Double)
        }
        (Value::Varchar(text), LogicalType::Uuid) => {
            Value::Varchar(text.clone()).cast(logical_type)
        }
        (Value::Varchar(text), LogicalType::Date) => parse_date_value(text),
        (Value::Varchar(text), LogicalType::Time) => parse_time_value(text),
        (Value::Varchar(text), LogicalType::Timestamp) => parse_timestamp_value(text),
        (Value::Varchar(text), LogicalType::TimestampTz) => parse_timestamptz_value(text),
        (Value::Varchar(text), LogicalType::Interval) => parse_interval_value(text),
        (Value::Varchar(text), LogicalType::Decimal { precision, scale }) => {
            parse_decimal_value(text, *precision, *scale)
        }
        (Value::Varchar(text), LogicalType::Blob) => parse_bytea_text(text).map(Value::Blob),
        (Value::Varchar(text), LogicalType::List(child)) => {
            parse_array_value(text, child.as_ref(), None)
        }
        (Value::Varchar(text), LogicalType::Array(child, size)) => {
            parse_array_value(text, child.as_ref(), Some(*size))
        }
        (Value::Varchar(_), LogicalType::Struct(_)) => Err(paro_error::not_implemented(
            "struct protocol parameters are not supported yet",
        )),
        _ => value.cast(logical_type),
    }
}

fn resolved_parameter_logical_type(value: &Value, declared: &LogicalType) -> LogicalType {
    match declared {
        LogicalType::Decimal {
            precision: 0,
            scale: 0,
        } => value.logical_type(),
        _ => declared.clone(),
    }
}

fn is_string_backed_parameter_type(logical_type: &LogicalType) -> bool {
    matches!(
        logical_type,
        LogicalType::Varchar
            | LogicalType::VarcharCollation(_)
            | LogicalType::Json
            | LogicalType::Jsonb
            | LogicalType::TsVector
            | LogicalType::TsQuery
    )
}

fn render_parameter_expr(value: &Value, logical_type: Option<&LogicalType>) -> Result<Expr> {
    let sql = match logical_type.filter(|ty| !matches!(ty, LogicalType::Unknown)) {
        Some(ty) => format!(
            "CAST({} AS {})",
            render_value_literal(value)?,
            sql_type_name(ty)?
        ),
        None => render_value_literal(value)?,
    };

    let tokens = tokenize_sql(&sql).map_err(|err| paro_error::from_parser(err.to_string()))?;
    parse_expr_tokens(&tokens).map_err(|err| paro_error::from_parser(err.to_string()))
}

fn render_value_literal(value: &Value) -> Result<String> {
    match value {
        Value::Null(_) => Ok("NULL".to_string()),
        Value::Boolean(v) => Ok(if *v { "TRUE" } else { "FALSE" }.to_string()),
        Value::TinyInt(v) => Ok(v.to_string()),
        Value::SmallInt(v) => Ok(v.to_string()),
        Value::Integer(v) => Ok(v.to_string()),
        Value::BigInt(v) => Ok(v.to_string()),
        Value::HugeInt(v) => Ok(v.to_string()),
        Value::UTinyInt(v) => Ok(v.to_string()),
        Value::USmallInt(v) => Ok(v.to_string()),
        Value::UInteger(v) => Ok(v.to_string()),
        Value::UBigInt(v) => Ok(v.to_string()),
        Value::UHugeInt(v) => Ok(v.to_string()),
        Value::Float(v) => Ok(v.to_string()),
        Value::Double(v) => Ok(v.to_string()),
        Value::Decimal(value, _precision, scale) => Ok(
            paro_common::runtime_value::format_decimal_i128(*value, *scale),
        ),
        Value::Varchar(v) => Ok(format!("'{}'", v.replace('\'', "''"))),
        Value::Blob(_) => Err(paro_error::not_implemented(
            "blob protocol parameters are not supported yet",
        )),
        Value::Uuid(v) => Ok(format!("'{}'", format_uuid(*v))),
        Value::Date(days) => Ok(format!(
            "'{}'",
            paro_common::runtime_value::format_date_days(i64::from(*days))
        )),
        Value::Timestamp(micros) => Ok(format!(
            "'{}'",
            paro_common::runtime_value::format_timestamp_micros(*micros)
        )),
        Value::TimestampTz(micros) => {
            let text = if *micros == i64::MAX {
                "infinity".to_string()
            } else if *micros == i64::MIN {
                "-infinity".to_string()
            } else {
                format!(
                    "{}+00",
                    paro_common::runtime_value::format_timestamp_micros(*micros)
                )
            };
            Ok(format!("'{text}'"))
        }
        Value::Time(_) => Ok(format!("'{}'", value)),
        Value::Interval(_, _, _) => Err(paro_error::not_implemented(
            "interval protocol parameters are not supported yet",
        )),
        Value::List(_, _) | Value::Struct(_, _) | Value::Array(_, _, _) => Err(
            paro_error::not_implemented("nested protocol parameters are not supported yet"),
        ),
    }
}

fn sql_type_name(logical_type: &LogicalType) -> Result<String> {
    let sql = match logical_type {
        LogicalType::Boolean => "BOOLEAN".to_string(),
        LogicalType::TinyInt | LogicalType::SmallInt => "SMALLINT".to_string(),
        LogicalType::Integer | LogicalType::UTinyInt | LogicalType::USmallInt => {
            "INTEGER".to_string()
        }
        LogicalType::BigInt
        | LogicalType::HugeInt
        | LogicalType::UInteger
        | LogicalType::UBigInt
        | LogicalType::UHugeInt => "BIGINT".to_string(),
        LogicalType::Float => "REAL".to_string(),
        LogicalType::Double => "DOUBLE".to_string(),
        LogicalType::Decimal { precision, scale } => format!("DECIMAL({precision}, {scale})"),
        LogicalType::Varchar
        | LogicalType::VarcharCollation(_)
        | LogicalType::Json
        | LogicalType::Jsonb
        | LogicalType::TsVector
        | LogicalType::TsQuery
        | LogicalType::StringLiteral => "VARCHAR".to_string(),
        LogicalType::Blob => "BYTEA".to_string(),
        LogicalType::Uuid => "UUID".to_string(),
        LogicalType::Date => "DATE".to_string(),
        LogicalType::Timestamp => "TIMESTAMP".to_string(),
        LogicalType::TimestampTz => "TIMESTAMPTZ".to_string(),
        LogicalType::Time => "TIME".to_string(),
        LogicalType::Interval => "INTERVAL".to_string(),
        LogicalType::Null | LogicalType::Unknown | LogicalType::IntegerLiteral(_) => {
            return Err(paro_error::syntax(
                "parameter type must be known before binding".to_string(),
            ))
        }
        LogicalType::Array(_, _) | LogicalType::List(_) | LogicalType::Struct(_) => {
            return Err(paro_error::not_implemented(format!(
                "parameter type {logical_type} is not supported yet",
            )))
        }
    };
    Ok(sql)
}

fn infer_text_parameter_value(text: &str) -> Value {
    if let Ok(value) = parse_bool(text) {
        return Value::Boolean(value);
    }
    if let Ok(value) = parse_i64(text, "bigint") {
        return Value::BigInt(value);
    }
    if let Ok(value) = parse_f64(text, "double precision") {
        return Value::Double(value);
    }
    Value::Varchar(text.to_string())
}

fn parse_bool(text: &str) -> Result<bool> {
    match text.trim().to_ascii_lowercase().as_str() {
        "t" | "true" | "1" | "on" => Ok(true),
        "f" | "false" | "0" | "off" => Ok(false),
        _ => Err(paro_error::invalid_value("boolean", text)),
    }
}

fn parse_i64(text: &str, ty: &str) -> Result<i64> {
    text.trim()
        .parse::<i64>()
        .map_err(|_| paro_error::invalid_value(ty, text))
}

fn parse_f64(text: &str, ty: &str) -> Result<f64> {
    text.trim()
        .parse::<f64>()
        .map_err(|_| paro_error::invalid_value(ty, text))
}

fn parse_exact_integer(text: &str, ty: &str) -> Result<i64> {
    text.trim()
        .parse::<i64>()
        .map_err(|_| paro_error::invalid_value(ty, text))
}

fn parse_exact_unsigned(text: &str, ty: &str) -> Result<u64> {
    text.trim()
        .parse::<u64>()
        .map_err(|_| paro_error::invalid_value(ty, text))
}

fn parse_hugeint(text: &str) -> Result<i128> {
    text.trim()
        .parse::<i128>()
        .map_err(|_| paro_error::invalid_value("hugeint", text))
}

fn parse_uhugeint(text: &str) -> Result<u128> {
    text.trim()
        .parse::<u128>()
        .map_err(|_| paro_error::invalid_value("uhugeint", text))
}

fn parse_date_value(text: &str) -> Result<Value> {
    let days = parse_date_text(text).ok_or_else(|| paro_error::invalid_value("date", text))?;
    let days = i32::try_from(days).map_err(|_| paro_error::invalid_value("date", text))?;
    Ok(Value::Date(days))
}

fn parse_time_value(text: &str) -> Result<Value> {
    let micros = parse_time_text(text).ok_or_else(|| paro_error::invalid_value("time", text))?;
    Ok(Value::Time(micros))
}

fn parse_timestamp_value(text: &str) -> Result<Value> {
    let micros =
        parse_timestamp_text(text).ok_or_else(|| paro_error::invalid_value("timestamp", text))?;
    Ok(Value::Timestamp(micros))
}

fn parse_timestamptz_value(text: &str) -> Result<Value> {
    let micros = parse_timestamptz_text(text)
        .ok_or_else(|| paro_error::invalid_value("timestamptz", text))?;
    Ok(Value::TimestampTz(micros))
}

fn parse_interval_value(text: &str) -> Result<Value> {
    let interval =
        parse_interval_text(text).ok_or_else(|| paro_error::invalid_value("interval", text))?;
    Ok(Value::Interval(
        interval.months,
        interval.days,
        interval.micros,
    ))
}

fn parse_decimal_value(text: &str, declared_precision: u8, declared_scale: u8) -> Result<Value> {
    let (value, actual_precision, actual_scale) = parse_decimal_components(text)?;
    if declared_precision == 0 && declared_scale == 0 {
        return Ok(Value::Decimal(value, actual_precision, actual_scale));
    }
    if actual_scale > declared_scale {
        return Err(paro_error::invalid_value("decimal", text));
    }
    let scaled_value = value
        .checked_mul(pow10_i128(declared_scale - actual_scale)?)
        .ok_or_else(|| paro_error::invalid_value("decimal", text))?;
    let total_digits = decimal_digit_count(scaled_value)
        .max(usize::from(declared_scale))
        .try_into()
        .unwrap_or(u8::MAX);
    if total_digits > declared_precision {
        return Err(paro_error::invalid_value("decimal", text));
    }
    Ok(Value::Decimal(
        scaled_value,
        declared_precision,
        declared_scale,
    ))
}

fn parse_decimal_components(text: &str) -> Result<(i128, u8, u8)> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(paro_error::invalid_value("decimal", text));
    }

    let sign = if trimmed.starts_with('-') {
        -1i128
    } else {
        1i128
    };
    let unsigned = trimmed.strip_prefix(['+', '-']).unwrap_or(trimmed);
    let mut parts = unsigned.split('.');
    let integer_part = parts.next().unwrap_or_default();
    let fractional_part = parts.next().unwrap_or_default();
    if parts.next().is_some()
        || integer_part.is_empty() && fractional_part.is_empty()
        || !integer_part.chars().all(|ch| ch.is_ascii_digit())
        || !fractional_part.chars().all(|ch| ch.is_ascii_digit())
    {
        return Err(paro_error::invalid_value("decimal", text));
    }

    let digits = format!("{integer_part}{fractional_part}");
    let raw = if digits.is_empty() {
        0
    } else {
        digits
            .parse::<i128>()
            .map_err(|_| paro_error::invalid_value("decimal", text))?
    };
    let value = raw
        .checked_mul(sign)
        .ok_or_else(|| paro_error::invalid_value("decimal", text))?;

    let integer_digits = integer_part.trim_start_matches('0');
    let significant_integer_digits = if integer_digits.is_empty() && !fractional_part.is_empty() {
        0
    } else {
        integer_digits.len().max(1)
    };
    let precision = (significant_integer_digits + fractional_part.len())
        .max(1)
        .try_into()
        .map_err(|_| paro_error::invalid_value("decimal", text))?;
    let scale = fractional_part
        .len()
        .try_into()
        .map_err(|_| paro_error::invalid_value("decimal", text))?;
    Ok((value, precision, scale))
}

fn pow10_i128(power: u8) -> Result<i128> {
    let mut value = 1i128;
    for _ in 0..power {
        value = value
            .checked_mul(10)
            .ok_or_else(|| paro_error::invalid_value("decimal", power.to_string()))?;
    }
    Ok(value)
}

fn decimal_digit_count(value: i128) -> usize {
    value.unsigned_abs().to_string().len()
}

fn parse_bytea_text(text: &str) -> Result<Vec<u8>> {
    let trimmed = text.trim();
    if let Some(hex) = trimmed
        .strip_prefix("\\x")
        .or_else(|| trimmed.strip_prefix("\\X"))
    {
        if hex.len() % 2 != 0 {
            return Err(paro_error::invalid_value("bytea", text));
        }
        let mut out = Vec::with_capacity(hex.len() / 2);
        for idx in (0..hex.len()).step_by(2) {
            let byte = u8::from_str_radix(&hex[idx..idx + 2], 16)
                .map_err(|_| paro_error::invalid_value("bytea", text))?;
            out.push(byte);
        }
        return Ok(out);
    }

    let bytes = trimmed.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut idx = 0usize;
    while idx < bytes.len() {
        if bytes[idx] != b'\\' {
            out.push(bytes[idx]);
            idx += 1;
            continue;
        }
        if idx + 1 >= bytes.len() {
            return Err(paro_error::invalid_value("bytea", text));
        }
        if bytes[idx + 1] == b'\\' {
            out.push(b'\\');
            idx += 2;
            continue;
        }
        if idx + 3 >= bytes.len()
            || !bytes[idx + 1..idx + 4]
                .iter()
                .all(|byte| (b'0'..=b'7').contains(byte))
        {
            return Err(paro_error::invalid_value("bytea", text));
        }
        let octal = std::str::from_utf8(&bytes[idx + 1..idx + 4])
            .map_err(|_| paro_error::invalid_value("bytea", text))?;
        out.push(
            u8::from_str_radix(octal, 8).map_err(|_| paro_error::invalid_value("bytea", text))?,
        );
        idx += 4;
    }
    Ok(out)
}

fn parse_array_value(
    text: &str,
    element_type: &LogicalType,
    fixed_size: Option<usize>,
) -> Result<Value> {
    let elements = parse_array_elements(text, element_type)?;
    if let Some(size) = fixed_size {
        if elements.len() != size {
            return Err(paro_error::invalid_value(format!("array[{size}]"), text));
        }
        return Ok(Value::Array(elements, element_type.clone(), size));
    }
    Ok(Value::List(elements, element_type.clone()))
}

fn parse_array_elements(text: &str, element_type: &LogicalType) -> Result<Vec<Value>> {
    let trimmed = text.trim();
    let inner = trimmed
        .strip_prefix('{')
        .and_then(|body| body.strip_suffix('}'))
        .ok_or_else(|| paro_error::invalid_value("array", text))?;
    if inner.is_empty() {
        return Ok(Vec::new());
    }

    let mut elements = Vec::new();
    let mut current = String::new();
    let mut depth = 0usize;
    let mut in_quotes = false;
    let mut escaped = false;

    for ch in inner.chars() {
        if in_quotes {
            current.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_quotes = false;
            }
            continue;
        }

        match ch {
            '"' => {
                in_quotes = true;
                current.push(ch);
            }
            '{' => {
                depth += 1;
                current.push(ch);
            }
            '}' => {
                if depth == 0 {
                    return Err(paro_error::invalid_value("array", text));
                }
                depth -= 1;
                current.push(ch);
            }
            ',' if depth == 0 => {
                elements.push(parse_array_element(&current, element_type)?);
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    if in_quotes || depth != 0 {
        return Err(paro_error::invalid_value("array", text));
    }
    elements.push(parse_array_element(&current, element_type)?);
    Ok(elements)
}

fn parse_array_element(token: &str, element_type: &LogicalType) -> Result<Value> {
    let trimmed = token.trim();
    if trimmed.eq_ignore_ascii_case("NULL") && !is_quoted_array_token(trimmed) {
        return Ok(Value::Null(element_type.clone()));
    }
    match element_type {
        LogicalType::List(child) => parse_array_value(trimmed, child.as_ref(), None),
        LogicalType::Array(child, size) => parse_array_value(trimmed, child.as_ref(), Some(*size)),
        LogicalType::Struct(_) => Err(paro_error::not_implemented(
            "struct protocol parameters are not supported yet",
        )),
        _ => {
            let scalar = unescape_array_token(trimmed)?;
            cast_parameter_value(&Value::Varchar(scalar), element_type)
        }
    }
}

fn is_quoted_array_token(token: &str) -> bool {
    token.starts_with('"') && token.ends_with('"') && token.len() >= 2
}

fn unescape_array_token(token: &str) -> Result<String> {
    if !is_quoted_array_token(token) {
        return Ok(token.to_string());
    }
    let mut out = String::with_capacity(token.len().saturating_sub(2));
    let mut escaped = false;
    for ch in token[1..token.len() - 1].chars() {
        if escaped {
            out.push(ch);
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else {
            out.push(ch);
        }
    }
    if escaped {
        return Err(paro_error::invalid_value("array", token));
    }
    Ok(out)
}

fn format_uuid(value: u128) -> String {
    let bytes = value.to_be_bytes();
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_oid_text_parameters_infer_decimal_precision_and_scale() {
        let declared = vec![Some(LogicalType::Decimal {
            precision: 0,
            scale: 0,
        })];
        let value = parse_text_parameter_value(Some(b"12.340"), declared[0].as_ref()).unwrap();
        let env = typed_parameter_env_from_values(&declared, &[value]).unwrap();
        let param = env.get(0).unwrap();
        assert_eq!(
            param.logical_type,
            LogicalType::Decimal {
                precision: 5,
                scale: 3,
            }
        );
        assert_eq!(param.value, Value::Decimal(12_340, 5, 3));
    }

    #[test]
    fn blob_interval_and_nested_text_parameters_parse_without_stringify() {
        let values = vec![
            parse_text_parameter_value(Some(br"\x4142"), Some(&LogicalType::Blob)).unwrap(),
            parse_text_parameter_value(
                Some(b"1 year 2 days 03:04:05"),
                Some(&LogicalType::Interval),
            )
            .unwrap(),
            parse_text_parameter_value(
                Some(br#"{1,2,3}"#),
                Some(&LogicalType::List(Box::new(LogicalType::Integer))),
            )
            .unwrap(),
            parse_text_parameter_value(
                Some(br#"{1,2}"#),
                Some(&LogicalType::Array(Box::new(LogicalType::Integer), 2)),
            )
            .unwrap(),
        ];
        let declared = vec![
            Some(LogicalType::Blob),
            Some(LogicalType::Interval),
            Some(LogicalType::List(Box::new(LogicalType::Integer))),
            Some(LogicalType::Array(Box::new(LogicalType::Integer), 2)),
        ];
        let env = typed_parameter_env_from_values(&declared, &values).unwrap();

        assert_eq!(env.get(0).unwrap().value, Value::Blob(b"AB".to_vec()));
        assert!(matches!(
            env.get(1).unwrap().value,
            Value::Interval(_, _, _)
        ));
        assert_eq!(
            env.get(2).unwrap().value,
            Value::List(
                vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
                LogicalType::Integer,
            )
        );
        assert_eq!(
            env.get(3).unwrap().value,
            Value::Array(
                vec![Value::Integer(1), Value::Integer(2)],
                LogicalType::Integer,
                2,
            )
        );
    }

    #[test]
    fn fixed_array_parameters_enforce_dimension() {
        let value = parse_text_parameter_value(
            Some(br#"{1,2,3}"#),
            Some(&LogicalType::Array(Box::new(LogicalType::Integer), 2)),
        )
        .unwrap();
        let err = typed_parameter_env_from_values(
            &[Some(LogicalType::Array(Box::new(LogicalType::Integer), 2))],
            &[value],
        )
        .unwrap_err();
        assert!(err.message().contains("array[2]"));
    }
}
