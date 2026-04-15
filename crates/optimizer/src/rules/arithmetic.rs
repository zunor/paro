//! Simplify arithmetic expressions with obvious results.
//!
//! Supported rewrites include:
//! - `x + 0` → `x`
//! - `x - 0` → `x`
//! - `x * 1` → `x`
//! - `x * 0` → `0`
//! - `x / 1` → `x`
//! - `x / 0` → `NULL`

use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_planner::expression::{ConstantExpression, Expression};
use paro_planner::operator::LogicalOperator;

use super::expression_matcher::ExpressionMatcher;
use super::rule::{Rule, RuleResult};

/// Matches arithmetic expressions with at least one constant child.
pub struct ArithmeticSimplificationMatcher;

impl ExpressionMatcher for ArithmeticSimplificationMatcher {
    fn matches<'a>(&self, expr: &'a Expression, bindings: &mut Vec<&'a Expression>) -> bool {
        let Expression::Function(func) = expr else {
            return false;
        };

        // Only match arithmetic operators
        let name = func.function.name.as_str();
        if !matches!(name, "+" | "-" | "*" | "/" | "//") {
            return false;
        }

        // Must have exactly 2 children
        if func.children.len() != 2 {
            return false;
        }

        // At least one child must be a constant
        let has_constant = func
            .children
            .iter()
            .any(|c| matches!(c, Expression::Constant(_)));
        if !has_constant {
            return false;
        }

        // Only match numeric types
        if !is_numeric_type(&func.return_type) {
            return false;
        }

        bindings.push(expr);
        true
    }
}

/// Check if a type is numeric (integer or floating point).
fn is_numeric_type(ty: &LogicalType) -> bool {
    matches!(
        ty,
        LogicalType::TinyInt
            | LogicalType::SmallInt
            | LogicalType::Integer
            | LogicalType::BigInt
            | LogicalType::HugeInt
            | LogicalType::UTinyInt
            | LogicalType::USmallInt
            | LogicalType::UInteger
            | LogicalType::UBigInt
            | LogicalType::UHugeInt
            | LogicalType::Float
            | LogicalType::Double
    )
}

/// Arithmetic simplification optimization rule.
///
/// Simplifies arithmetic expressions with known results.
pub struct ArithmeticSimplificationRule {
    matcher: ArithmeticSimplificationMatcher,
}

impl ArithmeticSimplificationRule {
    pub fn new() -> Self {
        Self {
            matcher: ArithmeticSimplificationMatcher,
        }
    }
}

impl Default for ArithmeticSimplificationRule {
    fn default() -> Self {
        Self::new()
    }
}

impl Rule for ArithmeticSimplificationRule {
    fn matcher(&self) -> &dyn ExpressionMatcher {
        &self.matcher
    }

    fn apply(
        &self,
        _op: &LogicalOperator,
        bindings: Vec<&Expression>,
        _is_root: bool,
    ) -> RuleResult {
        if bindings.is_empty() {
            return RuleResult::NoChange;
        }

        let Expression::Function(func) = bindings[0] else {
            return RuleResult::NoChange;
        };

        let name = func.function.name.as_str();
        let return_type = func.return_type.clone();

        // Get both children
        let left = &func.children[0];
        let right = &func.children[1];

        // Check for NULL in either operand
        if let Expression::Constant(c) = left {
            if c.value.is_null() {
                return RuleResult::Changed(Box::new(Expression::Constant(ConstantExpression {
                    value: Value::Null(return_type),
                    return_type: func.return_type.clone(),
                })));
            }
        }
        if let Expression::Constant(c) = right {
            if c.value.is_null() {
                return RuleResult::Changed(Box::new(Expression::Constant(ConstantExpression {
                    value: Value::Null(return_type),
                    return_type: func.return_type.clone(),
                })));
            }
        }
        match name {
            "+" => simplify_add_expr(left, right),
            "-" => simplify_subtract_expr(left, right),
            "*" => simplify_multiply_expr(left, right, &return_type),
            "/" | "//" => simplify_divide_expr(left, right, &return_type),
            _ => RuleResult::NoChange,
        }
    }

    fn name(&self) -> &'static str {
        "ArithmeticSimplificationRule"
    }
}

/// Simplify addition: x + 0 = x, 0 + x = x
fn simplify_add_expr(left: &Expression, right: &Expression) -> RuleResult {
    // Check if right is 0
    if let Expression::Constant(c) = right {
        if is_zero(&c.value) {
            return RuleResult::Changed(Box::new(left.clone()));
        }
    }
    // Check if left is 0
    if let Expression::Constant(c) = left {
        if is_zero(&c.value) {
            return RuleResult::Changed(Box::new(right.clone()));
        }
    }
    RuleResult::NoChange
}

/// Simplify subtraction: x - 0 = x (but NOT 0 - x)
fn simplify_subtract_expr(left: &Expression, right: &Expression) -> RuleResult {
    // Only simplify when 0 is on the right: x - 0 = x
    if let Expression::Constant(c) = right {
        if is_zero(&c.value) {
            return RuleResult::Changed(Box::new(left.clone()));
        }
    }
    RuleResult::NoChange
}

/// Simplify multiplication: x * 1 = x, x * 0 = 0
fn simplify_multiply_expr(
    left: &Expression,
    right: &Expression,
    return_type: &LogicalType,
) -> RuleResult {
    // Check right operand
    if let Expression::Constant(c) = right {
        if is_one(&c.value) {
            return RuleResult::Changed(Box::new(left.clone()));
        }
        if is_zero(&c.value) {
            return RuleResult::Changed(Box::new(Expression::Constant(ConstantExpression {
                value: make_zero(return_type),
                return_type: return_type.clone(),
            })));
        }
    }
    // Check left operand
    if let Expression::Constant(c) = left {
        if is_one(&c.value) {
            return RuleResult::Changed(Box::new(right.clone()));
        }
        if is_zero(&c.value) {
            return RuleResult::Changed(Box::new(Expression::Constant(ConstantExpression {
                value: make_zero(return_type),
                return_type: return_type.clone(),
            })));
        }
    }
    RuleResult::NoChange
}

/// Simplify division: x / 1 = x, x / 0 = NULL
fn simplify_divide_expr(
    left: &Expression,
    right: &Expression,
    return_type: &LogicalType,
) -> RuleResult {
    // Only simplify when constant is on the right (divisor)
    if let Expression::Constant(c) = right {
        if is_one(&c.value) {
            return RuleResult::Changed(Box::new(left.clone()));
        }
        if is_zero(&c.value) {
            return RuleResult::Changed(Box::new(Expression::Constant(ConstantExpression {
                value: Value::Null(return_type.clone()),
                return_type: return_type.clone(),
            })));
        }
    }
    RuleResult::NoChange
}

/// Check if a value is zero.
fn is_zero(value: &Value) -> bool {
    match value {
        Value::TinyInt(v) => *v == 0,
        Value::SmallInt(v) => *v == 0,
        Value::Integer(v) => *v == 0,
        Value::BigInt(v) => *v == 0,
        Value::HugeInt(v) => *v == 0,
        Value::UTinyInt(v) => *v == 0,
        Value::USmallInt(v) => *v == 0,
        Value::UInteger(v) => *v == 0,
        Value::UBigInt(v) => *v == 0,
        Value::UHugeInt(v) => *v == 0,
        Value::Float(v) => *v == 0.0,
        Value::Double(v) => *v == 0.0,
        _ => false,
    }
}

/// Check if a value is one.
fn is_one(value: &Value) -> bool {
    match value {
        Value::TinyInt(v) => *v == 1,
        Value::SmallInt(v) => *v == 1,
        Value::Integer(v) => *v == 1,
        Value::BigInt(v) => *v == 1,
        Value::HugeInt(v) => *v == 1,
        Value::UTinyInt(v) => *v == 1,
        Value::USmallInt(v) => *v == 1,
        Value::UInteger(v) => *v == 1,
        Value::UBigInt(v) => *v == 1,
        Value::UHugeInt(v) => *v == 1,
        Value::Float(v) => *v == 1.0,
        Value::Double(v) => *v == 1.0,
        _ => false,
    }
}

/// Create a zero value of the given type.
fn make_zero(ty: &LogicalType) -> Value {
    match ty {
        LogicalType::TinyInt => Value::TinyInt(0),
        LogicalType::SmallInt => Value::SmallInt(0),
        LogicalType::Integer => Value::Integer(0),
        LogicalType::BigInt => Value::BigInt(0),
        LogicalType::HugeInt => Value::HugeInt(0),
        LogicalType::UTinyInt => Value::UTinyInt(0),
        LogicalType::USmallInt => Value::USmallInt(0),
        LogicalType::UInteger => Value::UInteger(0),
        LogicalType::UBigInt => Value::UBigInt(0),
        LogicalType::UHugeInt => Value::UHugeInt(0),
        LogicalType::Float => Value::Float(0.0),
        LogicalType::Double => Value::Double(0.0),
        _ => Value::Integer(0), // fallback
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::expression_matcher::ExpressionMatcher;
    use paro_common::chunk::Chunk;
    use paro_common::vector::Vector;
    use paro_function::scalar::{ExpressionState, ScalarFunction};
    use paro_planner::expression::FunctionExpression;

    fn dummy_fn(
        _input: &Chunk,
        _state: &dyn ExpressionState,
        _result: &mut Vector,
    ) -> paro_common::error::Result<()> {
        Ok(())
    }

    fn make_constant(value: i32) -> Expression {
        Expression::Constant(ConstantExpression {
            value: Value::Integer(value),
            return_type: LogicalType::Integer,
        })
    }

    fn make_add(left: Expression, right: Expression) -> Expression {
        Expression::Function(FunctionExpression {
            function: ScalarFunction::new(
                "+".to_string(),
                vec![LogicalType::Integer, LogicalType::Integer],
                LogicalType::Integer,
                dummy_fn,
            )
            .into(),
            children: vec![left, right],
            return_type: LogicalType::Integer,
        })
    }

    fn make_subtract(left: Expression, right: Expression) -> Expression {
        Expression::Function(FunctionExpression {
            function: ScalarFunction::new(
                "-".to_string(),
                vec![LogicalType::Integer, LogicalType::Integer],
                LogicalType::Integer,
                dummy_fn,
            )
            .into(),
            children: vec![left, right],
            return_type: LogicalType::Integer,
        })
    }

    fn make_multiply(left: Expression, right: Expression) -> Expression {
        Expression::Function(FunctionExpression {
            function: ScalarFunction::new(
                "*".to_string(),
                vec![LogicalType::Integer, LogicalType::Integer],
                LogicalType::Integer,
                dummy_fn,
            )
            .into(),
            children: vec![left, right],
            return_type: LogicalType::Integer,
        })
    }

    fn make_divide(left: Expression, right: Expression) -> Expression {
        Expression::Function(FunctionExpression {
            function: ScalarFunction::new(
                "/".to_string(),
                vec![LogicalType::Integer, LogicalType::Integer],
                LogicalType::Integer,
                dummy_fn,
            )
            .into(),
            children: vec![left, right],
            return_type: LogicalType::Integer,
        })
    }

    #[test]
    fn test_matcher_matches_arithmetic_with_constant() {
        let matcher = ArithmeticSimplificationMatcher;

        // x + 0 should match
        let expr = make_add(make_constant(5), make_constant(0));
        let mut bindings = Vec::new();
        assert!(matcher.matches(&expr, &mut bindings));
    }

    #[test]
    fn test_matcher_rejects_non_arithmetic() {
        let matcher = ArithmeticSimplificationMatcher;

        // Pure constant should not match
        let expr = make_constant(42);
        let mut bindings = Vec::new();
        assert!(!matcher.matches(&expr, &mut bindings));
    }

    #[test]
    fn test_add_zero_right() {
        let rule = ArithmeticSimplificationRule::new();

        // 5 + 0 → 5
        let expr = make_add(make_constant(5), make_constant(0));
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        match result {
            RuleResult::Changed(expr) => match *expr {
                Expression::Constant(c) => {
                    assert_eq!(c.value, Value::Integer(5));
                }
                _ => panic!("Expected Constant"),
            },
            _ => panic!("Expected Changed result"),
        }
    }

    #[test]
    fn test_add_zero_left() {
        let rule = ArithmeticSimplificationRule::new();

        // 0 + 5 → 5
        let expr = make_add(make_constant(0), make_constant(5));
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        match result {
            RuleResult::Changed(expr) => match *expr {
                Expression::Constant(c) => {
                    assert_eq!(c.value, Value::Integer(5));
                }
                _ => panic!("Expected Constant"),
            },
            _ => panic!("Expected Changed result"),
        }
    }

    #[test]
    fn test_subtract_zero() {
        let rule = ArithmeticSimplificationRule::new();

        // 5 - 0 → 5
        let expr = make_subtract(make_constant(5), make_constant(0));
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        match result {
            RuleResult::Changed(expr) => match *expr {
                Expression::Constant(c) => {
                    assert_eq!(c.value, Value::Integer(5));
                }
                _ => panic!("Expected Constant"),
            },
            _ => panic!("Expected Changed result"),
        }
    }

    #[test]
    fn test_subtract_zero_left_no_change() {
        let rule = ArithmeticSimplificationRule::new();

        // 0 - 5 should NOT simplify (result is -5, not 5)
        let expr = make_subtract(make_constant(0), make_constant(5));
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        assert!(matches!(result, RuleResult::NoChange));
    }

    #[test]
    fn test_multiply_by_one() {
        let rule = ArithmeticSimplificationRule::new();

        // 5 * 1 → 5
        let expr = make_multiply(make_constant(5), make_constant(1));
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        match result {
            RuleResult::Changed(expr) => match *expr {
                Expression::Constant(c) => {
                    assert_eq!(c.value, Value::Integer(5));
                }
                _ => panic!("Expected Constant"),
            },
            _ => panic!("Expected Changed result"),
        }
    }

    #[test]
    fn test_multiply_by_zero() {
        let rule = ArithmeticSimplificationRule::new();

        // 5 * 0 → 0
        let expr = make_multiply(make_constant(5), make_constant(0));
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        match result {
            RuleResult::Changed(expr) => match *expr {
                Expression::Constant(c) => {
                    assert_eq!(c.value, Value::Integer(0));
                }
                _ => panic!("Expected Constant"),
            },
            _ => panic!("Expected Changed result"),
        }
    }

    #[test]
    fn test_divide_by_one() {
        let rule = ArithmeticSimplificationRule::new();

        // 5 / 1 → 5
        let expr = make_divide(make_constant(5), make_constant(1));
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        match result {
            RuleResult::Changed(expr) => match *expr {
                Expression::Constant(c) => {
                    assert_eq!(c.value, Value::Integer(5));
                }
                _ => panic!("Expected Constant"),
            },
            _ => panic!("Expected Changed result"),
        }
    }

    #[test]
    fn test_divide_by_zero() {
        let rule = ArithmeticSimplificationRule::new();

        // 5 / 0 → NULL
        let expr = make_divide(make_constant(5), make_constant(0));
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        match result {
            RuleResult::Changed(expr) => match *expr {
                Expression::Constant(c) => {
                    assert!(c.value.is_null());
                }
                _ => panic!("Expected Constant"),
            },
            _ => panic!("Expected Changed result with NULL"),
        }
    }

    #[test]
    fn test_arithmetic_with_null() {
        let rule = ArithmeticSimplificationRule::new();

        // 5 + NULL → NULL
        let null_const = Expression::Constant(ConstantExpression {
            value: Value::Null(LogicalType::Integer),
            return_type: LogicalType::Integer,
        });
        let expr = make_add(make_constant(5), null_const);
        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        match result {
            RuleResult::Changed(expr) => match *expr {
                Expression::Constant(c) => {
                    assert!(c.value.is_null());
                }
                _ => panic!("Expected Constant"),
            },
            _ => panic!("Expected Changed result with NULL"),
        }
    }

    #[test]
    fn test_rule_name() {
        let rule = ArithmeticSimplificationRule::new();
        assert_eq!(rule.name(), "ArithmeticSimplificationRule");
    }
}
