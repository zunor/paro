// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Exact decimal arithmetic shared by scalar and aggregate functions.

use ethnum::i256;
use paro_common::error::{self as paro_error, Result};

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
