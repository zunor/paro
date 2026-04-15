//! Date arithmetic functions.
//!
//!

use crate::{ExpressionState, ScalarFunction, ScalarFunctionSet};
use paro_common::chunk::Chunk;
use paro_common::error::Result;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

const MICROS_PER_DAY: i64 = 24 * 60 * 60 * 1_000_000;

fn date_add_days_impl(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let count = input.size();
    let date_vec = input
        .column(0)
        .ok_or_else(|| paro_common::error::internal("Missing date column".to_string()))?;
    let days_vec = input
        .column(1)
        .ok_or_else(|| paro_common::error::internal("Missing days column".to_string()))?;
    result.set_count(count);
    for i in 0..count {
        if date_vec.is_null(i) || days_vec.is_null(i) {
            result.validity_mut().set_null(i);
        } else {
            let date_days = date_vec.get_i64(i).unwrap_or(0);
            let add_days = days_vec.get_i64(i).unwrap_or(0);
            result.set_i64(i, date_days + add_days);
        }
    }
    Ok(())
}

pub fn get_date_add_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("date_add".to_string());
    set.add_function(ScalarFunction::new(
        "date_add".to_string(),
        vec![LogicalType::Date, LogicalType::BigInt],
        LogicalType::Date,
        date_add_days_impl,
    ));
    set
}

fn date_sub_days_impl(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let count = input.size();
    let date_vec = input
        .column(0)
        .ok_or_else(|| paro_common::error::internal("Missing date column".to_string()))?;
    let days_vec = input
        .column(1)
        .ok_or_else(|| paro_common::error::internal("Missing days column".to_string()))?;
    result.set_count(count);
    for i in 0..count {
        if date_vec.is_null(i) || days_vec.is_null(i) {
            result.validity_mut().set_null(i);
        } else {
            let date_days = date_vec.get_i64(i).unwrap_or(0);
            let sub_days = days_vec.get_i64(i).unwrap_or(0);
            result.set_i64(i, date_days - sub_days);
        }
    }
    Ok(())
}

pub fn get_date_sub_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("date_sub".to_string());
    set.add_function(ScalarFunction::new(
        "date_sub".to_string(),
        vec![LogicalType::Date, LogicalType::BigInt],
        LogicalType::Date,
        date_sub_days_impl,
    ));
    set
}

fn datediff_date_impl(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let count = input.size();
    let start_vec = input
        .column(0)
        .ok_or_else(|| paro_common::error::internal("Missing start date column".to_string()))?;
    let end_vec = input
        .column(1)
        .ok_or_else(|| paro_common::error::internal("Missing end date column".to_string()))?;
    result.set_count(count);
    for i in 0..count {
        if start_vec.is_null(i) || end_vec.is_null(i) {
            result.validity_mut().set_null(i);
        } else {
            let start = start_vec.get_i64(i).unwrap_or(0);
            let end = end_vec.get_i64(i).unwrap_or(0);
            result.set_i64(i, end - start);
        }
    }
    Ok(())
}

fn datediff_timestamp_impl(
    input: &Chunk,
    _state: &dyn ExpressionState,
    result: &mut Vector,
) -> Result<()> {
    let count = input.size();
    let start_vec = input.column(0).ok_or_else(|| {
        paro_common::error::internal("Missing start timestamp column".to_string())
    })?;
    let end_vec = input
        .column(1)
        .ok_or_else(|| paro_common::error::internal("Missing end timestamp column".to_string()))?;
    result.set_count(count);
    for i in 0..count {
        if start_vec.is_null(i) || end_vec.is_null(i) {
            result.validity_mut().set_null(i);
        } else {
            let start = start_vec.get_i64(i).unwrap_or(0);
            let end = end_vec.get_i64(i).unwrap_or(0);
            result.set_i64(i, (end - start) / MICROS_PER_DAY);
        }
    }
    Ok(())
}

pub fn get_date_diff_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("datediff".to_string());
    set.add_function(ScalarFunction::new(
        "datediff".to_string(),
        vec![LogicalType::Date, LogicalType::Date],
        LogicalType::BigInt,
        datediff_date_impl,
    ));
    set.add_function(ScalarFunction::new(
        "datediff".to_string(),
        vec![LogicalType::Timestamp, LogicalType::Timestamp],
        LogicalType::BigInt,
        datediff_timestamp_impl,
    ));
    set
}

fn age_impl(input: &Chunk, _state: &dyn ExpressionState, result: &mut Vector) -> Result<()> {
    let count = input.size();
    let end_vec = input
        .column(0)
        .ok_or_else(|| paro_common::error::internal("Missing end timestamp column".to_string()))?;
    let start_vec = input.column(1).ok_or_else(|| {
        paro_common::error::internal("Missing start timestamp column".to_string())
    })?;
    result.set_count(count);
    for i in 0..count {
        if end_vec.is_null(i) || start_vec.is_null(i) {
            result.validity_mut().set_null(i);
        } else {
            let end = end_vec.get_i64(i).unwrap_or(0);
            let start = start_vec.get_i64(i).unwrap_or(0);
            result.set_i64(i, (end - start) / MICROS_PER_DAY);
        }
    }
    Ok(())
}

pub fn get_age_functions() -> ScalarFunctionSet {
    let mut set = ScalarFunctionSet::new("age".to_string());
    set.add_function(ScalarFunction::new(
        "age".to_string(),
        vec![LogicalType::Timestamp, LogicalType::Timestamp],
        LogicalType::BigInt,
        age_impl,
    ));
    set
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::Any;

    struct MockState;
    impl ExpressionState for MockState {
        fn current_database(&self) -> Option<&str> {
            None
        }
        fn current_schema(&self) -> Option<&str> {
            None
        }
        fn current_user(&self) -> Option<&str> {
            None
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[test]
    fn test_date_add() {
        let input_date = Vector::from_i64(&[0, 10, 100]);
        let input_days = Vector::from_i64(&[1, 5, -10]);
        let chunk = Chunk::from_vectors(vec![input_date, input_days]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Date);
        date_add_days_impl(&chunk, &state, &mut result).unwrap();
        assert_eq!(result.get_i64(0), Some(1));
        assert_eq!(result.get_i64(1), Some(15));
        assert_eq!(result.get_i64(2), Some(90));
    }

    #[test]
    fn test_date_sub() {
        let input_date = Vector::from_i64(&[10, 100, 50]);
        let input_days = Vector::from_i64(&[5, 10, 100]);
        let chunk = Chunk::from_vectors(vec![input_date, input_days]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::Date);
        date_sub_days_impl(&chunk, &state, &mut result).unwrap();
        assert_eq!(result.get_i64(0), Some(5));
        assert_eq!(result.get_i64(1), Some(90));
        assert_eq!(result.get_i64(2), Some(-50));
    }

    #[test]
    fn test_datediff() {
        let start = Vector::from_i64(&[0, 10, 100]);
        let end = Vector::from_i64(&[10, 20, 50]);
        let chunk = Chunk::from_vectors(vec![start, end]);
        let state = MockState;
        let mut result = Vector::new(LogicalType::BigInt);
        datediff_date_impl(&chunk, &state, &mut result).unwrap();
        assert_eq!(result.get_i64(0), Some(10));
        assert_eq!(result.get_i64(1), Some(10));
        assert_eq!(result.get_i64(2), Some(-50));
    }
}
