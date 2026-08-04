// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Decimal Cast Functions
//!
//! Implements casts between Decimal and numeric/varchar types.

use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::{format_decimal_i128, Value};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use crate::scalar::executor::varlen::VarcharResultWriter;

use super::{BindCastInput, BoundCastInfo, CastExecCtx};

const DECIMAL_MAX_PRECISION: u8 = 38;

fn pow10_i128(exp: u8) -> Result<i128> {
    if exp > DECIMAL_MAX_PRECISION {
        return Err(paro_error::out_of_range(format!(
            "Decimal scale {} exceeds max precision {}",
            exp, DECIMAL_MAX_PRECISION
        )));
    }
    let mut value: i128 = 1;
    for _ in 0..exp {
        value = value
            .checked_mul(10)
            .ok_or_else(|| paro_error::out_of_range("Decimal scale overflow"))?;
    }
    Ok(value)
}

fn decimal_limit(precision: u8) -> Result<i128> {
    if precision == 0 {
        return Err(paro_error::invalid_input("Decimal precision must be > 0"));
    }
    pow10_i128(precision)
}

fn check_decimal_precision(value: i128, precision: u8) -> Result<()> {
    let limit = decimal_limit(precision)?;
    let abs = value
        .checked_abs()
        .ok_or_else(|| paro_error::out_of_range("Decimal value overflow"))?;
    if abs >= limit {
        return Err(paro_error::out_of_range(format!(
            "Decimal value {} exceeds precision {}",
            value, precision
        )));
    }
    Ok(())
}

fn round_divide(value: i128, divisor: i128) -> i128 {
    let mut quotient = value / divisor;
    let remainder = value % divisor;
    if remainder == 0 {
        return quotient;
    }
    let rem_abs = remainder.abs();
    if rem_abs * 2 >= divisor {
        if value >= 0 {
            quotient += 1;
        } else {
            quotient -= 1;
        }
    }
    quotient
}

fn rescale_decimal(value: i128, from_scale: u8, to_scale: u8) -> Result<i128> {
    if from_scale == to_scale {
        return Ok(value);
    }
    if to_scale > from_scale {
        let diff = to_scale - from_scale;
        let factor = pow10_i128(diff)?;
        return value
            .checked_mul(factor)
            .ok_or_else(|| paro_error::out_of_range("Decimal scale overflow"));
    }

    let diff = from_scale - to_scale;
    let divisor = pow10_i128(diff)?;
    Ok(round_divide(value, divisor))
}

fn parse_decimal_string(s: &str) -> Result<(i128, u8)> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err(paro_error::invalid_value("DECIMAL", s));
    }
    let cleaned = trimmed.replace('_', "");
    if cleaned.contains('e') || cleaned.contains('E') {
        return Err(paro_error::invalid_value("DECIMAL", s));
    }

    let mut sign = 1i128;
    let mut rest = cleaned.as_str();
    if let Some(stripped) = rest.strip_prefix('-') {
        sign = -1;
        rest = stripped;
    } else if let Some(stripped) = rest.strip_prefix('+') {
        rest = stripped;
    }

    let mut parts = rest.splitn(3, '.');
    let int_part = parts.next().unwrap_or("");
    let frac_part = parts.next().unwrap_or("");
    if parts.next().is_some() {
        return Err(paro_error::invalid_value("DECIMAL", s));
    }

    let digits = format!("{}{}", int_part, frac_part);
    if digits.is_empty() {
        return Err(paro_error::invalid_value("DECIMAL", s));
    }
    if digits.len() > DECIMAL_MAX_PRECISION as usize {
        return Err(paro_error::out_of_range("Decimal literal too large"));
    }
    if !digits.chars().all(|c| c.is_ascii_digit()) {
        return Err(paro_error::invalid_value("DECIMAL", s));
    }

    let unsigned =
        i128::from_str_radix(&digits, 10).map_err(|_| paro_error::invalid_value("DECIMAL", s))?;
    let value = sign * unsigned;
    let scale = frac_part.len() as u8;

    Ok((value, scale))
}

/// Parse a textual decimal using the same rescaling and precision rules as
/// the vectorized VARCHAR -> DECIMAL cast.
///
/// Text-backed table functions (COPY/CSV, JSON, etc.) should use this helper
/// instead of maintaining their own decimal grammar. Keeping the conversion
/// here guarantees that bulk ingestion and SQL casts agree on rounding and
/// overflow behavior.
pub(crate) fn parse_decimal_text(s: &str, precision: u8, scale: u8) -> Result<Value> {
    let (raw_value, raw_scale) = parse_decimal_string(s)?;
    let scaled = rescale_decimal(raw_value, raw_scale, scale)?;
    check_decimal_precision(scaled, precision)?;
    Ok(Value::Decimal(scaled, precision, scale))
}

fn decimal_params(ty: &LogicalType) -> Result<(u8, u8)> {
    match ty {
        LogicalType::Decimal { precision, scale } => Ok((*precision, *scale)),
        _ => Err(paro_error::internal("Expected Decimal logical type")),
    }
}

fn decimal_value_from_vector(vector: &Vector, idx: usize, precision: u8) -> Result<i128> {
    let value = if precision <= 18 {
        vector
            .get_i64(idx)
            .ok_or_else(|| paro_error::internal("Failed to read Decimal(i64)"))? as i128
    } else {
        vector
            .get_i128(idx)
            .ok_or_else(|| paro_error::internal("Failed to read Decimal(i128)"))?
    };
    Ok(value)
}

fn set_decimal_value(
    result: &mut Vector,
    idx: usize,
    precision: u8,
    value: i128,
    ctx: &CastExecCtx<'_>,
    all_success: &mut bool,
) -> Result<()> {
    if let Err(err) = check_decimal_precision(value, precision) {
        if ctx.try_cast {
            result.set_null(idx, true);
            *all_success = false;
            return Ok(());
        }
        return Err(err);
    }

    let write_res = if precision <= 18 {
        i64::try_from(value)
            .map(|v| unsafe { result.set_flat::<i64>(idx, v) })
            .map_err(|_| paro_error::out_of_range("Decimal value exceeds i64 range"))
    } else {
        unsafe { result.set_flat::<i128>(idx, value) };
        Ok(())
    };

    if let Err(err) = write_res {
        if ctx.try_cast {
            result.set_null(idx, true);
            *all_success = false;
            return Ok(());
        }
        return Err(err);
    }

    Ok(())
}

pub fn numeric_to_decimal_cast(
    input: &Vector,
    result: &mut Vector,
    count: usize,
    ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    let (precision, scale) = decimal_params(result.logical_type())?;
    let factor = pow10_i128(scale)?;

    let mut all_success = true;
    result.set_count(count);

    for i in 0..count {
        if input.is_null(i) {
            result.set_null(i, true);
            continue;
        }

        let raw = match input.logical_type() {
            LogicalType::TinyInt => input.get_i8(i).unwrap() as i128,
            LogicalType::SmallInt => input.get_i16(i).unwrap() as i128,
            LogicalType::Integer => input.get_i32(i).unwrap() as i128,
            LogicalType::BigInt => input.get_i64(i).unwrap() as i128,
            LogicalType::HugeInt => input.get_i128(i).unwrap(),
            LogicalType::UTinyInt => input.get_u8(i).unwrap() as i128,
            LogicalType::USmallInt => input.get_u16(i).unwrap() as i128,
            LogicalType::UInteger => input.get_u32(i).unwrap() as i128,
            LogicalType::UBigInt => input.get_u64(i).unwrap() as i128,
            LogicalType::UHugeInt => {
                let v = input.get_u128(i).unwrap();
                if v > i128::MAX as u128 {
                    if ctx.try_cast {
                        result.set_null(i, true);
                        all_success = false;
                        continue;
                    }
                    return Err(paro_error::out_of_range(
                        "Unsigned value exceeds Decimal range",
                    ));
                }
                v as i128
            }
            _ => {
                return Err(paro_error::internal(
                    "numeric_to_decimal_cast: unsupported source type",
                ))
            }
        };

        let scaled = match raw.checked_mul(factor) {
            Some(v) => v,
            None => {
                if ctx.try_cast {
                    result.set_null(i, true);
                    all_success = false;
                    continue;
                }
                return Err(paro_error::out_of_range("Decimal scale overflow"));
            }
        };

        set_decimal_value(result, i, precision, scaled, ctx, &mut all_success)?;
    }

    Ok(all_success)
}

pub fn float_to_decimal_cast(
    input: &Vector,
    result: &mut Vector,
    count: usize,
    ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    let (precision, scale) = decimal_params(result.logical_type())?;
    let factor = 10f64.powi(scale as i32);

    let mut all_success = true;
    result.set_count(count);

    for i in 0..count {
        if input.is_null(i) {
            result.set_null(i, true);
            continue;
        }

        let value = match input.logical_type() {
            LogicalType::Float => input.get_f32(i).unwrap() as f64,
            LogicalType::Double => input.get_f64(i).unwrap(),
            _ => {
                return Err(paro_error::internal(
                    "float_to_decimal_cast: unsupported source type",
                ))
            }
        };

        if !value.is_finite() {
            if ctx.try_cast {
                result.set_null(i, true);
                all_success = false;
                continue;
            }
            return Err(paro_error::out_of_range("Float value is not finite"));
        }

        let scaled = (value * factor).round();
        if scaled < i128::MIN as f64 || scaled > i128::MAX as f64 {
            if ctx.try_cast {
                result.set_null(i, true);
                all_success = false;
                continue;
            }
            return Err(paro_error::out_of_range("Decimal scale overflow"));
        }

        let scaled_i128 = scaled as i128;
        set_decimal_value(result, i, precision, scaled_i128, ctx, &mut all_success)?;
    }

    Ok(all_success)
}

pub fn varchar_to_decimal_cast(
    input: &Vector,
    result: &mut Vector,
    count: usize,
    ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    let (precision, scale) = decimal_params(result.logical_type())?;
    let mut all_success = true;
    let view = input.try_to_varlen_view(count)?;
    result.set_count(count);

    for row in 0..count {
        if !view.is_valid(row) {
            result.set_null(row, true);
            continue;
        }

        let source_value = view.get_inline_string(row);
        let s = source_value.as_str();

        let (raw_value, raw_scale) = match parse_decimal_string(s) {
            Ok(v) => v,
            Err(err) => {
                if ctx.try_cast {
                    result.set_null(row, true);
                    all_success = false;
                    continue;
                }
                return Err(err);
            }
        };

        let scaled = match rescale_decimal(raw_value, raw_scale, scale) {
            Ok(v) => v,
            Err(err) => {
                if ctx.try_cast {
                    result.set_null(row, true);
                    all_success = false;
                    continue;
                }
                return Err(err);
            }
        };

        set_decimal_value(result, row, precision, scaled, ctx, &mut all_success)?;
    }

    Ok(all_success)
}

pub fn decimal_to_decimal_cast(
    input: &Vector,
    result: &mut Vector,
    count: usize,
    ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    let (source_precision, source_scale) = decimal_params(input.logical_type())?;
    let (target_precision, target_scale) = decimal_params(result.logical_type())?;

    let mut all_success = true;
    result.set_count(count);

    for i in 0..count {
        if input.is_null(i) {
            result.set_null(i, true);
            continue;
        }

        let value = decimal_value_from_vector(input, i, source_precision)?;
        let scaled = match rescale_decimal(value, source_scale, target_scale) {
            Ok(v) => v,
            Err(err) => {
                if ctx.try_cast {
                    result.set_null(i, true);
                    all_success = false;
                    continue;
                }
                return Err(err);
            }
        };

        set_decimal_value(result, i, target_precision, scaled, ctx, &mut all_success)?;
    }

    Ok(all_success)
}

pub fn decimal_to_varchar_cast(
    input: &Vector,
    result: &mut Vector,
    count: usize,
    _ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    let (precision, scale) = decimal_params(input.logical_type())?;
    let mut writer = VarcharResultWriter::new(result, count);

    for row in 0..count {
        if input.is_null(row) {
            writer.set_null(row);
            continue;
        }
        let value = decimal_value_from_vector(input, row, precision)?;
        let s = format_decimal_i128(value, scale);
        writer.write_str(row, &s)?;
    }

    Ok(true)
}

pub fn decimal_to_numeric_cast(
    input: &Vector,
    result: &mut Vector,
    count: usize,
    ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    let (precision, scale) = decimal_params(input.logical_type())?;
    let mut all_success = true;
    result.set_count(count);

    for i in 0..count {
        if input.is_null(i) {
            result.set_null(i, true);
            continue;
        }

        let value = decimal_value_from_vector(input, i, precision)?;
        let rescaled = match rescale_decimal(value, scale, 0) {
            Ok(v) => v,
            Err(err) => {
                if ctx.try_cast {
                    result.set_null(i, true);
                    all_success = false;
                    continue;
                }
                return Err(err);
            }
        };

        let write_res = match result.logical_type() {
            LogicalType::TinyInt => i8::try_from(rescaled)
                .map(|v| unsafe { result.set_flat::<i8>(i, v) })
                .map_err(|_| paro_error::out_of_range("Decimal value exceeds i8 range")),
            LogicalType::SmallInt => i16::try_from(rescaled)
                .map(|v| unsafe { result.set_flat::<i16>(i, v) })
                .map_err(|_| paro_error::out_of_range("Decimal value exceeds i16 range")),
            LogicalType::Integer => i32::try_from(rescaled)
                .map(|v| unsafe { result.set_flat::<i32>(i, v) })
                .map_err(|_| paro_error::out_of_range("Decimal value exceeds i32 range")),
            LogicalType::BigInt => i64::try_from(rescaled)
                .map(|v| unsafe { result.set_flat::<i64>(i, v) })
                .map_err(|_| paro_error::out_of_range("Decimal value exceeds i64 range")),
            LogicalType::HugeInt => {
                unsafe { result.set_flat::<i128>(i, rescaled) };
                Ok(())
            }
            LogicalType::UTinyInt => u8::try_from(rescaled)
                .map(|v| unsafe { result.set_flat::<u8>(i, v) })
                .map_err(|_| paro_error::out_of_range("Decimal value exceeds u8 range")),
            LogicalType::USmallInt => u16::try_from(rescaled)
                .map(|v| unsafe { result.set_flat::<u16>(i, v) })
                .map_err(|_| paro_error::out_of_range("Decimal value exceeds u16 range")),
            LogicalType::UInteger => u32::try_from(rescaled)
                .map(|v| unsafe { result.set_flat::<u32>(i, v) })
                .map_err(|_| paro_error::out_of_range("Decimal value exceeds u32 range")),
            LogicalType::UBigInt => u64::try_from(rescaled)
                .map(|v| unsafe { result.set_flat::<u64>(i, v) })
                .map_err(|_| paro_error::out_of_range("Decimal value exceeds u64 range")),
            LogicalType::UHugeInt => {
                let v = u128::try_from(rescaled)
                    .map_err(|_| paro_error::out_of_range("Decimal value exceeds u128 range"))?;
                unsafe { result.set_flat::<u128>(i, v) };
                Ok(())
            }
            _ => Err(paro_error::internal(
                "decimal_to_numeric_cast: unsupported target type",
            )),
        };

        if let Err(err) = write_res {
            if ctx.try_cast {
                result.set_null(i, true);
                all_success = false;
                continue;
            }
            return Err(err);
        }
    }

    Ok(all_success)
}

pub fn decimal_to_float_cast(
    input: &Vector,
    result: &mut Vector,
    count: usize,
    _ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    let (precision, scale) = decimal_params(input.logical_type())?;
    let denom = 10f64.powi(scale as i32);

    result.set_count(count);

    for i in 0..count {
        if input.is_null(i) {
            result.set_null(i, true);
            continue;
        }

        let value = decimal_value_from_vector(input, i, precision)? as f64 / denom;
        match result.logical_type() {
            LogicalType::Float => unsafe { result.set_flat::<f32>(i, value as f32) },
            LogicalType::Double => unsafe { result.set_flat::<f64>(i, value) },
            _ => {
                return Err(paro_error::internal(
                    "decimal_to_float_cast: unsupported target type",
                ))
            }
        }
    }

    Ok(true)
}

pub fn bind_decimal_casts(
    _input: &BindCastInput,
    source: &LogicalType,
    target: &LogicalType,
) -> Result<Option<BoundCastInfo>> {
    use LogicalType::*;

    let info = match (source, target) {
        (Decimal { .. }, Decimal { .. }) => BoundCastInfo::fixed(decimal_to_decimal_cast),
        (Decimal { .. }, Varchar) => BoundCastInfo::varlen(decimal_to_varchar_cast),
        (Varchar, Decimal { .. }) => BoundCastInfo::varlen(varchar_to_decimal_cast),

        (Decimal { .. }, TinyInt)
        | (Decimal { .. }, SmallInt)
        | (Decimal { .. }, Integer)
        | (Decimal { .. }, BigInt)
        | (Decimal { .. }, HugeInt)
        | (Decimal { .. }, UTinyInt)
        | (Decimal { .. }, USmallInt)
        | (Decimal { .. }, UInteger)
        | (Decimal { .. }, UBigInt)
        | (Decimal { .. }, UHugeInt) => BoundCastInfo::fixed(decimal_to_numeric_cast),

        (Decimal { .. }, Float) | (Decimal { .. }, Double) => {
            BoundCastInfo::fixed(decimal_to_float_cast)
        }

        (TinyInt, Decimal { .. })
        | (SmallInt, Decimal { .. })
        | (Integer, Decimal { .. })
        | (BigInt, Decimal { .. })
        | (HugeInt, Decimal { .. })
        | (UTinyInt, Decimal { .. })
        | (USmallInt, Decimal { .. })
        | (UInteger, Decimal { .. })
        | (UBigInt, Decimal { .. })
        | (UHugeInt, Decimal { .. }) => BoundCastInfo::fixed(numeric_to_decimal_cast),

        (Float, Decimal { .. }) | (Double, Decimal { .. }) => {
            BoundCastInfo::fixed(float_to_decimal_cast)
        }

        _ => return Ok(None),
    };

    Ok(Some(info))
}
