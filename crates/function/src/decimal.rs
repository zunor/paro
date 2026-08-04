// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Exact decimal arithmetic shared by scalar and aggregate functions.

use ethnum::i256;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

pub(crate) const MAX_DECIMAL_PRECISION: u8 = 38;

pub(crate) fn pow10(exp: u8) -> Result<i256> {
    // Two DECIMAL(38) operands can require up to 76 decimal digits while an
    // expression is being evaluated. The signed i256 range covers 10^76.
    if exp > MAX_DECIMAL_PRECISION * 2 {
        return Err(paro_error::out_of_range(format!(
            "Decimal scale {exp} exceeds the exact intermediate range"
        )));
    }
    (0..exp).try_fold(i256::ONE, |value, _| {
        value
            .checked_mul(i256::from(10))
            .ok_or_else(|| paro_error::out_of_range("Decimal scale overflow"))
    })
}

pub(crate) fn rescale(value: i256, from_scale: u8, to_scale: u8) -> Result<i256> {
    if from_scale == to_scale {
        return Ok(value);
    }
    if from_scale < to_scale {
        return value
            .checked_mul(pow10(to_scale - from_scale)?)
            .ok_or_else(|| paro_error::out_of_range("Decimal scale overflow"));
    }
    round_divide(value, pow10(from_scale - to_scale)?)
}

pub(crate) fn round_divide(value: i256, divisor: i256) -> Result<i256> {
    let (quotient, remainder) = value
        .checked_div_rem(divisor)
        .ok_or_else(|| paro_error::out_of_range("Decimal division overflow"))?;
    if remainder == i256::ZERO {
        return Ok(quotient);
    }

    let divisor_abs = divisor
        .checked_abs()
        .ok_or_else(|| paro_error::out_of_range("Decimal divisor overflow"))?;
    let remainder_abs = remainder
        .checked_abs()
        .ok_or_else(|| paro_error::out_of_range("Decimal remainder overflow"))?;
    let threshold = divisor_abs / i256::from(2) + divisor_abs % i256::from(2);
    if remainder_abs < threshold {
        return Ok(quotient);
    }

    let adjustment = if (value < i256::ZERO) == (divisor < i256::ZERO) {
        i256::ONE
    } else {
        i256::MINUS_ONE
    };
    quotient
        .checked_add(adjustment)
        .ok_or_else(|| paro_error::out_of_range("Decimal rounding overflow"))
}

pub(crate) fn check_precision(value: i256, precision: u8) -> Result<()> {
    if precision == 0 || precision > MAX_DECIMAL_PRECISION {
        return Err(paro_error::invalid_input(format!(
            "Decimal precision must be between 1 and {MAX_DECIMAL_PRECISION}"
        )));
    }
    let absolute = value
        .checked_abs()
        .ok_or_else(|| paro_error::out_of_range("Decimal value overflow"))?;
    if absolute >= pow10(precision)? {
        return Err(paro_error::out_of_range(format!(
            "Decimal value exceeds precision {precision}"
        )));
    }
    Ok(())
}

pub(crate) fn to_i128(value: i256, precision: u8) -> Result<i128> {
    check_precision(value, precision)?;
    i128::try_from(value)
        .map_err(|_| paro_error::out_of_range("Decimal value exceeds the physical i128 range"))
}

/// Read a non-null DECIMAL value through flat, constant, or dictionary storage.
///
/// # Safety
/// `row` must be in bounds for `vector`.
pub(crate) unsafe fn read_decimal(vector: &Vector, row: usize) -> (i128, u8) {
    match vector.logical_type() {
        LogicalType::Decimal { precision, scale } if *precision <= 18 => {
            (vector.get_fixed::<i64>(row) as i128, *scale)
        }
        LogicalType::Decimal { scale, .. } => (vector.get_fixed::<i128>(row), *scale),
        ty => unreachable!("DECIMAL reader received {ty}"),
    }
}

/// Write a DECIMAL value using the physical width declared by the result vector.
pub(crate) fn write_decimal(result: &mut Vector, row: usize, value: i128) -> Result<()> {
    let LogicalType::Decimal { precision, .. } = result.logical_type() else {
        return Err(paro_error::internal(
            "DECIMAL writer received a non-DECIMAL result vector",
        ));
    };
    if *precision <= 18 {
        result.set_i64(
            row,
            i64::try_from(value).map_err(|_| {
                paro_error::out_of_range("Decimal value exceeds the physical i64 range")
            })?,
        );
    } else {
        result.set_i128(row, value);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn wide_intermediate_preserves_decimal_product() {
        let value = pow10(38).unwrap() - i256::ONE;
        let product = value.checked_mul(value).unwrap();
        assert!(product > i256::from(i128::MAX));
        assert_eq!(
            rescale(product, 76, 38).unwrap(),
            pow10(38).unwrap() - i256::from(2)
        );
    }

    #[test]
    fn precision_check_rejects_38_digit_overflow() {
        assert!(check_precision(pow10(38).unwrap() - i256::ONE, 38).is_ok());
        assert!(check_precision(pow10(38).unwrap(), 38).is_err());
    }

    #[test]
    fn decimal_physical_io_uses_declared_width() {
        let allocator = paro_common::test_utils::test_allocator();
        let mut narrow = Vector::try_new(
            LogicalType::Decimal {
                precision: 18,
                scale: 2,
            },
            2,
            allocator.clone(),
        )
        .unwrap();
        narrow.set_count(2);
        write_decimal(&mut narrow, 0, 12_345).unwrap();
        write_decimal(&mut narrow, 1, 67_890).unwrap();
        assert_eq!(unsafe { read_decimal(&narrow, 0) }, (12_345, 2));
        let selection =
            paro_common::vector::SelectionVector::try_from_indices(vec![1, 0], allocator.clone())
                .unwrap();
        let dictionary = Vector::try_dictionary(Arc::new(narrow), selection).unwrap();
        assert_eq!(unsafe { read_decimal(&dictionary, 0) }, (67_890, 2));

        let mut wide = Vector::try_new(
            LogicalType::Decimal {
                precision: 38,
                scale: 4,
            },
            1,
            allocator,
        )
        .unwrap();
        wide.set_count(1);
        write_decimal(&mut wide, 0, i128::MAX / 2).unwrap();
        assert_eq!(unsafe { read_decimal(&wide, 0) }, (i128::MAX / 2, 4));
    }
}
