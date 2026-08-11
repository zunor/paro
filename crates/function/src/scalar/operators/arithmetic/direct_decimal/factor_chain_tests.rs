// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::sync::Arc;

use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use super::super::{
    register_arithmetic_functions, try_decimal_factor_fusion, BoundScalarFunction,
    DecimalOperandSide, ExpressionState, FunctionData, ScalarBindInput, ScalarFunctionSet,
};

struct BindState {
    bind_data: Arc<dyn FunctionData>,
}

impl ExpressionState for BindState {
    fn current_database(&self) -> Option<&str> {
        None
    }

    fn current_schema(&self) -> Option<&str> {
        None
    }

    fn current_user(&self) -> Option<&str> {
        None
    }

    fn bind_data(&self) -> Option<&dyn FunctionData> {
        Some(self.bind_data.as_ref())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn bind_decimal_operator(name: &str, arguments: &[LogicalType]) -> BoundScalarFunction {
    let mut set = ScalarFunctionSet::new(name.to_string());
    register_arithmetic_functions(&mut set);
    let (function, target_types) = set.bind(arguments).unwrap();
    function
        .bind(&ScalarBindInput::new(
            target_types,
            vec![None; arguments.len()],
        ))
        .unwrap()
}

fn execute_fused_factor(function: &BoundScalarFunction, outer: Vector, inner: Vector) -> Vector {
    let state = BindState {
        bind_data: function.bind_data.as_ref().unwrap().clone(),
    };
    let chunk = paro_common::test_utils::test_chunk_from_vectors(vec![outer, inner]);
    let mut result = paro_common::test_utils::test_vector(function.return_type.clone());
    function.execute(&chunk, &state, &mut result).unwrap();
    result
}

#[test]
fn handles_integer_constant_nulls_and_exact_fallback() {
    let discount_type = LogicalType::Decimal {
        precision: 4,
        scale: 2,
    };
    let price_type = LogicalType::Decimal {
        precision: 15,
        scale: 2,
    };
    let inner = bind_decimal_operator("-", &[LogicalType::Integer, discount_type.clone()]);
    let outer = bind_decimal_operator("*", &[price_type.clone(), inner.return_type.clone()]);
    let fused = try_decimal_factor_fusion(
        &outer,
        &inner,
        &Value::Integer(1),
        &price_type,
        &discount_type,
        &LogicalType::Integer,
        DecimalOperandSide::Right,
        DecimalOperandSide::Left,
    )
    .expect("factor shape should fuse");

    let mut prices = paro_common::test_utils::test_vector(price_type);
    prices.set_count(3);
    prices.set_i64(0, 10_000);
    prices.set_i64(1, 20_000);
    prices.set_i64(2, 30_000);
    let mut discounts = paro_common::test_utils::test_vector(discount_type);
    discounts.set_count(3);
    discounts.set_i64(0, 5);
    discounts.set_i64(1, 7);
    discounts.set_i64(2, 10);
    let direct_result = execute_fused_factor(&fused, prices.reference(), discounts.reference());
    assert_eq!(unsafe { direct_result.get_fixed::<i128>(0) }, 950_000);
    assert_eq!(unsafe { direct_result.get_fixed::<i128>(1) }, 1_860_000);
    assert_eq!(unsafe { direct_result.get_fixed::<i128>(2) }, 2_700_000);

    discounts.set_null(1, true);
    let result = execute_fused_factor(&fused, prices, discounts);
    assert_eq!(unsafe { result.get_fixed::<i128>(0) }, 950_000);
    assert!(result.is_null(1));
    assert_eq!(unsafe { result.get_fixed::<i128>(2) }, 2_700_000);

    let wide_price_type = LogicalType::Decimal {
        precision: 30,
        scale: 2,
    };
    let outer = bind_decimal_operator("*", &[wide_price_type.clone(), inner.return_type.clone()]);
    let fused = try_decimal_factor_fusion(
        &outer,
        &inner,
        &Value::Integer(1),
        &wide_price_type,
        &LogicalType::Decimal {
            precision: 4,
            scale: 2,
        },
        &LogicalType::Integer,
        DecimalOperandSide::Right,
        DecimalOperandSide::Left,
    )
    .expect("wide factor shape should fuse");
    let mut prices = paro_common::test_utils::test_vector(wide_price_type);
    prices.set_count(1);
    prices.set_i128(0, 10_000_000_000_000_000_000);
    let mut discounts = paro_common::test_utils::test_vector(LogicalType::Decimal {
        precision: 4,
        scale: 2,
    });
    discounts.set_count(1);
    discounts.set_i64(0, 5);
    let result = execute_fused_factor(&fused, prices, discounts);
    assert_eq!(
        unsafe { result.get_fixed::<i128>(0) },
        950_000_000_000_000_000_000
    );
}
