// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::sync::Arc;

use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use super::super::{
    register_arithmetic_functions, try_decimal_factor_fusion, try_decimal_factor_product_fusion,
    try_execute_decimal_factor_chain, BoundScalarFunction, DecimalFactorChainPlan,
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

fn execute_fused_factor_product(function: &BoundScalarFunction, inputs: Vec<Vector>) -> Vector {
    let state = BindState {
        bind_data: function.bind_data.as_ref().unwrap().clone(),
    };
    let chunk = paro_common::test_utils::test_chunk_from_vectors(inputs);
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

#[test]
fn declines_decimal_constants_whose_value_scale_disagrees_with_declared_type() {
    let factor_type = LogicalType::Decimal {
        precision: 4,
        scale: 2,
    };
    let price_type = LogicalType::Decimal {
        precision: 15,
        scale: 2,
    };
    let inner = bind_decimal_operator("-", &[factor_type.clone(), factor_type.clone()]);
    let outer = bind_decimal_operator("*", &[price_type.clone(), inner.return_type.clone()]);

    assert!(try_decimal_factor_fusion(
        &outer,
        &inner,
        &Value::Decimal(100, 4, 3),
        &price_type,
        &factor_type,
        &factor_type,
        DecimalOperandSide::Right,
        DecimalOperandSide::Left,
    )
    .is_none());
}

#[test]
fn factor_product_kernel_matches_profit_expression_and_null_semantics() {
    let money_type = LogicalType::Decimal {
        precision: 15,
        scale: 2,
    };
    let rate_type = LogicalType::Decimal {
        precision: 4,
        scale: 2,
    };
    let factor_inner = bind_decimal_operator("-", &[LogicalType::Integer, rate_type.clone()]);
    let factor_outer =
        bind_decimal_operator("*", &[money_type.clone(), factor_inner.return_type.clone()]);
    let factor = try_decimal_factor_fusion(
        &factor_outer,
        &factor_inner,
        &Value::Integer(1),
        &money_type,
        &rate_type,
        &LogicalType::Integer,
        DecimalOperandSide::Right,
        DecimalOperandSide::Left,
    )
    .expect("discounted revenue should fuse");
    let product = bind_decimal_operator("*", &[money_type.clone(), money_type.clone()]);
    let outer = bind_decimal_operator(
        "-",
        &[factor.return_type.clone(), product.return_type.clone()],
    );
    let fused = try_decimal_factor_product_fusion(
        &outer,
        &factor,
        &product,
        &money_type,
        &money_type,
        DecimalOperandSide::Left,
    )
    .expect("profit difference-of-products should fuse");

    let mut prices = paro_common::test_utils::test_vector(money_type.clone());
    prices.set_count(2);
    prices.set_i64(0, 10_000);
    prices.set_i64(1, 20_000);
    let mut discounts = paro_common::test_utils::test_vector(rate_type);
    discounts.set_count(2);
    discounts.set_i64(0, 5);
    discounts.set_i64(1, 10);
    let mut costs = paro_common::test_utils::test_vector(money_type.clone());
    costs.set_count(2);
    costs.set_i64(0, 2_000);
    costs.set_i64(1, 5_000);
    let mut quantities = paro_common::test_utils::test_vector(money_type);
    quantities.set_count(2);
    quantities.set_i64(0, 300);
    quantities.set_i64(1, 200);

    let result = execute_fused_factor_product(
        &fused,
        vec![
            prices.reference(),
            discounts.reference(),
            costs.reference(),
            quantities.reference(),
        ],
    );
    assert_eq!(unsafe { result.get_fixed::<i128>(0) }, 350_000);
    assert_eq!(unsafe { result.get_fixed::<i128>(1) }, 800_000);

    discounts.set_null(1, true);
    let result = execute_fused_factor_product(&fused, vec![prices, discounts, costs, quantities]);
    assert_eq!(unsafe { result.get_fixed::<i128>(0) }, 350_000);
    assert!(result.is_null(1));
}

#[test]
fn factor_chain_plan_supports_a_right_hand_shared_operand() {
    let factor_type = LogicalType::Decimal {
        precision: 4,
        scale: 2,
    };
    let price_type = LogicalType::Decimal {
        precision: 15,
        scale: 2,
    };
    let producer_inner = bind_decimal_operator("-", &[LogicalType::Integer, factor_type.clone()]);
    let producer_outer = bind_decimal_operator(
        "*",
        &[price_type.clone(), producer_inner.return_type.clone()],
    );
    let producer = try_decimal_factor_fusion(
        &producer_outer,
        &producer_inner,
        &Value::Integer(1),
        &price_type,
        &factor_type,
        &LogicalType::Integer,
        DecimalOperandSide::Right,
        DecimalOperandSide::Left,
    )
    .expect("producer factor should fuse");

    let consumer_inner =
        bind_decimal_operator("+", &[LogicalType::Integer, producer.return_type.clone()]);
    let consumer_outer = bind_decimal_operator(
        "*",
        &[factor_type.clone(), consumer_inner.return_type.clone()],
    );
    let consumer = try_decimal_factor_fusion(
        &consumer_outer,
        &consumer_inner,
        &Value::Integer(1),
        &factor_type,
        &producer.return_type,
        &LogicalType::Integer,
        DecimalOperandSide::Right,
        DecimalOperandSide::Left,
    )
    .expect("consumer factor should fuse");
    let plan = DecimalFactorChainPlan::try_new(&producer, &consumer, DecimalOperandSide::Right)
        .expect("consumer right argument has the producer type");
    assert!(
        DecimalFactorChainPlan::try_new(&producer, &consumer, DecimalOperandSide::Left,).is_none()
    );

    let mut prices = paro_common::test_utils::test_vector(price_type);
    prices.set_count(2);
    prices.set_i64(0, 10_000);
    prices.set_i64(1, 20_000);
    let mut discounts = paro_common::test_utils::test_vector(factor_type.clone());
    discounts.set_count(2);
    discounts.set_i64(0, 5);
    discounts.set_i64(1, 10);
    let mut multipliers = paro_common::test_utils::test_vector(factor_type);
    multipliers.set_count(2);
    multipliers.set_i64(0, 200);
    multipliers.set_i64(1, 300);

    let expected_producer =
        execute_fused_factor(&producer, prices.reference(), discounts.reference());
    let expected_consumer = execute_fused_factor(
        &consumer,
        multipliers.reference(),
        expected_producer.reference(),
    );
    let mut actual_producer = paro_common::test_utils::test_vector(producer.return_type.clone());
    let mut actual_consumer = paro_common::test_utils::test_vector(consumer.return_type.clone());
    assert!(try_execute_decimal_factor_chain(
        plan,
        &prices,
        &discounts,
        &multipliers,
        &mut actual_producer,
        &mut actual_consumer,
        2,
    )
    .expect("factor chain execution"));
    actual_producer.set_count(2);
    actual_consumer.set_count(2);

    for row in 0..2 {
        assert_eq!(
            actual_producer.get_value(row),
            expected_producer.get_value(row)
        );
        assert_eq!(
            actual_consumer.get_value(row),
            expected_consumer.get_value(row)
        );
    }
}
