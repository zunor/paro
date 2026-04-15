//! Fold constant expressions at rewrite time.
//!
//! Examples: `2 + 3` → `5`, `1 = 1` → `true`, `'a' || 'b'` → `'ab'`.

use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_planner::expression::ComparisonType;
use paro_planner::expression::ConjunctionType;
use paro_planner::expression::{ConstantExpression, Expression};
use paro_planner::operator::LogicalOperator;

use super::expression_matcher::{ExpressionMatcher, FoldableConstantMatcher};
use super::rule::{Rule, RuleResult};

/// Matcher for constant folding that matches foldable expressions
/// but excludes already-constant expressions.
pub struct ConstantFoldingMatcher;

impl ExpressionMatcher for ConstantFoldingMatcher {
    fn matches<'a>(&self, expr: &'a Expression, bindings: &mut Vec<&'a Expression>) -> bool {
        // Don't match pure constants - they're already folded
        if matches!(expr, Expression::Constant(_)) {
            return false;
        }
        // Use FoldableConstantMatcher to check if expression is foldable
        FoldableConstantMatcher.matches(expr, bindings)
    }
}

/// Constant folding optimization rule.
///
/// Evaluates constant expressions at compile time and replaces them
/// with their computed values.
///
/// # Examples
/// - `2 + 3` → `5`
/// - `1 = 1` → `true`
/// - `'hello' || ' world'` → `'hello world'`
/// - `NOT true` → `false`
pub struct ConstantFoldingRule {
    matcher: ConstantFoldingMatcher,
}

impl ConstantFoldingRule {
    /// Create a new constant folding rule.
    pub fn new() -> Self {
        Self {
            matcher: ConstantFoldingMatcher,
        }
    }
}

impl Default for ConstantFoldingRule {
    fn default() -> Self {
        Self::new()
    }
}

impl Rule for ConstantFoldingRule {
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

        let expr = bindings[0];

        // Try to evaluate the expression to a constant
        match evaluate_constant(expr) {
            Some(value) => {
                let return_type = value.logical_type();
                RuleResult::Changed(Box::new(Expression::Constant(ConstantExpression {
                    value,
                    return_type,
                })))
            }
            None => RuleResult::NoChange,
        }
    }

    fn name(&self) -> &'static str {
        "ConstantFoldingRule"
    }
}

/// Evaluate a constant expression to a Value.
///
/// Returns None if the expression cannot be evaluated at compile time.
fn evaluate_constant(expr: &Expression) -> Option<Value> {
    match expr {
        Expression::Constant(c) => Some(c.value.clone()),

        Expression::Comparison(comp) => {
            let left = evaluate_constant(&comp.left)?;
            let right = evaluate_constant(&comp.right)?;
            evaluate_comparison(&left, &right, comp.comparison_type)
        }

        Expression::Conjunction(conj) => {
            let values: Option<Vec<Value>> = conj.children.iter().map(evaluate_constant).collect();
            let values = values?;
            evaluate_conjunction(&values, conj.conjunction_type)
        }

        Expression::Cast(cast) => {
            let child_value = evaluate_constant(&cast.child)?;
            child_value.cast(&cast.target_type).ok()
        }

        Expression::Operator(op) => {
            let values: Option<Vec<Value>> = op.children.iter().map(evaluate_constant).collect();
            let values = values?;
            evaluate_operator(&values, op.operator_type, &op.return_type)
        }

        Expression::Function(func) => {
            // For now, only fold simple arithmetic functions
            let values: Option<Vec<Value>> = func.children.iter().map(evaluate_constant).collect();
            let values = values?;
            evaluate_function(&func.function.name, &values, &func.return_type)
        }

        Expression::Case(case) => {
            let check = evaluate_constant(&case.check)?;
            match check {
                Value::Boolean(true) => evaluate_constant(&case.result_if_true),
                Value::Boolean(false) => evaluate_constant(&case.result_if_false),
                Value::Null(_) => evaluate_constant(&case.result_if_false),
                _ => None,
            }
        }

        // Cannot fold column refs, aggregates, subqueries, windows
        Expression::ColumnRef(_)
        | Expression::Reference(_)
        | Expression::Aggregate(_)
        | Expression::Subquery(_)
        | Expression::Window(_) => None,
    }
}

/// Evaluate a comparison operation on two constant values.
fn evaluate_comparison(left: &Value, right: &Value, comp_type: ComparisonType) -> Option<Value> {
    // Handle NULL comparisons
    if left.is_null() || right.is_null() {
        return match comp_type {
            ComparisonType::DistinctFrom => Some(Value::Boolean(left.is_null() != right.is_null())),
            ComparisonType::NotDistinctFrom => {
                Some(Value::Boolean(left.is_null() && right.is_null()))
            }
            // Regular comparisons with NULL return NULL
            _ => Some(Value::Null(LogicalType::Boolean)),
        };
    }

    let result = match comp_type {
        ComparisonType::Equal => left == right,
        ComparisonType::NotEqual => left != right,
        ComparisonType::LessThan => left.partial_cmp(right)? == std::cmp::Ordering::Less,
        ComparisonType::LessThanOrEqual => {
            matches!(
                left.partial_cmp(right)?,
                std::cmp::Ordering::Less | std::cmp::Ordering::Equal
            )
        }
        ComparisonType::GreaterThan => left.partial_cmp(right)? == std::cmp::Ordering::Greater,
        ComparisonType::GreaterThanOrEqual => {
            matches!(
                left.partial_cmp(right)?,
                std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
            )
        }
        ComparisonType::DistinctFrom => left != right,
        ComparisonType::NotDistinctFrom => left == right,
    };

    Some(Value::Boolean(result))
}

/// Evaluate a conjunction (AND/OR) operation on constant values.
fn evaluate_conjunction(values: &[Value], conj_type: ConjunctionType) -> Option<Value> {
    match conj_type {
        ConjunctionType::And => {
            let mut has_null = false;
            for v in values {
                match v {
                    Value::Boolean(false) => return Some(Value::Boolean(false)),
                    Value::Boolean(true) => {}
                    Value::Null(_) => has_null = true,
                    _ => return None,
                }
            }
            if has_null {
                Some(Value::Null(LogicalType::Boolean))
            } else {
                Some(Value::Boolean(true))
            }
        }
        ConjunctionType::Or => {
            let mut has_null = false;
            for v in values {
                match v {
                    Value::Boolean(true) => return Some(Value::Boolean(true)),
                    Value::Boolean(false) => {}
                    Value::Null(_) => has_null = true,
                    _ => return None,
                }
            }
            if has_null {
                Some(Value::Null(LogicalType::Boolean))
            } else {
                Some(Value::Boolean(false))
            }
        }
    }
}

/// Evaluate an operator expression on constant values.
fn evaluate_operator(
    values: &[Value],
    op_type: paro_planner::expression::OperatorType,
    _return_type: &LogicalType,
) -> Option<Value> {
    use paro_planner::expression::OperatorType;

    match op_type {
        OperatorType::Not => {
            if values.len() != 1 {
                return None;
            }
            match &values[0] {
                Value::Boolean(b) => Some(Value::Boolean(!b)),
                Value::Null(_) => Some(Value::Null(LogicalType::Boolean)),
                _ => None,
            }
        }
        OperatorType::IsNull => {
            if values.len() != 1 {
                return None;
            }
            Some(Value::Boolean(values[0].is_null()))
        }
        OperatorType::IsNotNull => {
            if values.len() != 1 {
                return None;
            }
            Some(Value::Boolean(!values[0].is_null()))
        }
        OperatorType::Coalesce => {
            for v in values {
                if !v.is_null() {
                    return Some(v.clone());
                }
            }
            // All values are NULL, return NULL with the return type
            values.first().cloned()
        }
        OperatorType::In | OperatorType::NotIn => {
            if values.is_empty() {
                return None;
            }
            let needle = &values[0];
            if needle.is_null() {
                return Some(Value::Null(LogicalType::Boolean));
            }
            let haystack = &values[1..];
            let mut has_null = false;
            for v in haystack {
                if v.is_null() {
                    has_null = true;
                } else if needle == v {
                    return Some(Value::Boolean(op_type == OperatorType::In));
                }
            }
            if has_null {
                Some(Value::Null(LogicalType::Boolean))
            } else {
                Some(Value::Boolean(op_type == OperatorType::NotIn))
            }
        }
        // LIKE/ILIKE require pattern matching, skip for now
        OperatorType::Like
        | OperatorType::ILike
        | OperatorType::ArrayConstructor
        | OperatorType::StructConstructor
        | OperatorType::ArrayExtract
        | OperatorType::ErrorIfMultipleRows => None,
    }
}

/// Evaluate a function call on constant values.
fn evaluate_function(name: &str, values: &[Value], return_type: &LogicalType) -> Option<Value> {
    // Handle NULL propagation for most functions
    if values.iter().any(|v| v.is_null()) {
        // Most functions return NULL if any argument is NULL
        return Some(Value::Null(return_type.clone()));
    }

    match name {
        // Arithmetic operators
        "+" => evaluate_add(values, return_type),
        "-" => evaluate_subtract(values, return_type),
        "*" => evaluate_multiply(values, return_type),
        "/" => evaluate_divide(values, return_type),
        "%" | "mod" => evaluate_modulo(values, return_type),

        // String functions
        "||" | "concat" => evaluate_concat(values),
        "length" | "char_length" | "character_length" => evaluate_length(values),
        "upper" => evaluate_upper(values),
        "lower" => evaluate_lower(values),

        // Math functions
        "abs" => evaluate_abs(values, return_type),
        "negate" => evaluate_negate(values, return_type),

        _ => None,
    }
}

/// Evaluate addition.
fn evaluate_add(values: &[Value], return_type: &LogicalType) -> Option<Value> {
    if values.len() != 2 {
        return None;
    }
    match (&values[0], &values[1]) {
        (Value::Integer(a), Value::Integer(b)) => Some(Value::Integer(a.checked_add(*b)?)),
        (Value::BigInt(a), Value::BigInt(b)) => Some(Value::BigInt(a.checked_add(*b)?)),
        (Value::Double(a), Value::Double(b)) => Some(Value::Double(a + b)),
        (Value::Float(a), Value::Float(b)) => Some(Value::Float(a + b)),
        // Cross-type arithmetic - cast to return type
        _ => {
            let a = values[0].cast(return_type).ok()?;
            let b = values[1].cast(return_type).ok()?;
            evaluate_add(&[a, b], return_type)
        }
    }
}

/// Evaluate subtraction.
fn evaluate_subtract(values: &[Value], return_type: &LogicalType) -> Option<Value> {
    if values.len() != 2 {
        return None;
    }
    match (&values[0], &values[1]) {
        (Value::Integer(a), Value::Integer(b)) => Some(Value::Integer(a.checked_sub(*b)?)),
        (Value::BigInt(a), Value::BigInt(b)) => Some(Value::BigInt(a.checked_sub(*b)?)),
        (Value::Double(a), Value::Double(b)) => Some(Value::Double(a - b)),
        (Value::Float(a), Value::Float(b)) => Some(Value::Float(a - b)),
        _ => {
            let a = values[0].cast(return_type).ok()?;
            let b = values[1].cast(return_type).ok()?;
            evaluate_subtract(&[a, b], return_type)
        }
    }
}

/// Evaluate multiplication.
fn evaluate_multiply(values: &[Value], return_type: &LogicalType) -> Option<Value> {
    if values.len() != 2 {
        return None;
    }
    match (&values[0], &values[1]) {
        (Value::Integer(a), Value::Integer(b)) => Some(Value::Integer(a.checked_mul(*b)?)),
        (Value::BigInt(a), Value::BigInt(b)) => Some(Value::BigInt(a.checked_mul(*b)?)),
        (Value::Double(a), Value::Double(b)) => Some(Value::Double(a * b)),
        (Value::Float(a), Value::Float(b)) => Some(Value::Float(a * b)),
        _ => {
            let a = values[0].cast(return_type).ok()?;
            let b = values[1].cast(return_type).ok()?;
            evaluate_multiply(&[a, b], return_type)
        }
    }
}

/// Evaluate division.
fn evaluate_divide(values: &[Value], return_type: &LogicalType) -> Option<Value> {
    if values.len() != 2 {
        return None;
    }
    match (&values[0], &values[1]) {
        (Value::Integer(a), Value::Integer(b)) => {
            if *b == 0 {
                None // Division by zero
            } else {
                Some(Value::Integer(a / b))
            }
        }
        (Value::BigInt(a), Value::BigInt(b)) => {
            if *b == 0 {
                None
            } else {
                Some(Value::BigInt(a / b))
            }
        }
        (Value::Double(a), Value::Double(b)) => {
            if *b == 0.0 {
                None
            } else {
                Some(Value::Double(a / b))
            }
        }
        (Value::Float(a), Value::Float(b)) => {
            if *b == 0.0 {
                None
            } else {
                Some(Value::Float(a / b))
            }
        }
        _ => {
            let a = values[0].cast(return_type).ok()?;
            let b = values[1].cast(return_type).ok()?;
            evaluate_divide(&[a, b], return_type)
        }
    }
}

/// Evaluate modulo.
fn evaluate_modulo(values: &[Value], return_type: &LogicalType) -> Option<Value> {
    if values.len() != 2 {
        return None;
    }
    match (&values[0], &values[1]) {
        (Value::Integer(a), Value::Integer(b)) => {
            if *b == 0 {
                None
            } else {
                Some(Value::Integer(a % b))
            }
        }
        (Value::BigInt(a), Value::BigInt(b)) => {
            if *b == 0 {
                None
            } else {
                Some(Value::BigInt(a % b))
            }
        }
        _ => {
            let a = values[0].cast(return_type).ok()?;
            let b = values[1].cast(return_type).ok()?;
            evaluate_modulo(&[a, b], return_type)
        }
    }
}

/// Evaluate string concatenation.
fn evaluate_concat(values: &[Value]) -> Option<Value> {
    let mut result = String::new();
    for v in values {
        match v {
            Value::Varchar(s) => result.push_str(s),
            _ => return None,
        }
    }
    Some(Value::Varchar(result))
}

/// Evaluate string length.
fn evaluate_length(values: &[Value]) -> Option<Value> {
    if values.len() != 1 {
        return None;
    }
    match &values[0] {
        Value::Varchar(s) => Some(Value::BigInt(s.chars().count() as i64)),
        _ => None,
    }
}

/// Evaluate upper case.
fn evaluate_upper(values: &[Value]) -> Option<Value> {
    if values.len() != 1 {
        return None;
    }
    match &values[0] {
        Value::Varchar(s) => Some(Value::Varchar(s.to_uppercase())),
        _ => None,
    }
}

/// Evaluate lower case.
fn evaluate_lower(values: &[Value]) -> Option<Value> {
    if values.len() != 1 {
        return None;
    }
    match &values[0] {
        Value::Varchar(s) => Some(Value::Varchar(s.to_lowercase())),
        _ => None,
    }
}

/// Evaluate absolute value.
fn evaluate_abs(values: &[Value], return_type: &LogicalType) -> Option<Value> {
    if values.len() != 1 {
        return None;
    }
    match &values[0] {
        Value::Integer(v) => Some(Value::Integer(v.abs())),
        Value::BigInt(v) => Some(Value::BigInt(v.abs())),
        Value::Double(v) => Some(Value::Double(v.abs())),
        Value::Float(v) => Some(Value::Float(v.abs())),
        _ => {
            let v = values[0].cast(return_type).ok()?;
            evaluate_abs(&[v], return_type)
        }
    }
}

/// Evaluate negation.
fn evaluate_negate(values: &[Value], return_type: &LogicalType) -> Option<Value> {
    if values.len() != 1 {
        return None;
    }
    match &values[0] {
        Value::Integer(v) => Some(Value::Integer(-v)),
        Value::BigInt(v) => Some(Value::BigInt(-v)),
        Value::Double(v) => Some(Value::Double(-v)),
        Value::Float(v) => Some(Value::Float(-v)),
        _ => {
            let v = values[0].cast(return_type).ok()?;
            evaluate_negate(&[v], return_type)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::expression_matcher::ExpressionMatcher;
    use paro_planner::expression::ComparisonExpression;
    use paro_planner::expression::ConjunctionExpression;

    fn make_constant(value: i32) -> Expression {
        Expression::Constant(ConstantExpression {
            value: Value::Integer(value),
            return_type: LogicalType::Integer,
        })
    }

    fn make_bool_constant(value: bool) -> Expression {
        Expression::Constant(ConstantExpression {
            value: Value::Boolean(value),
            return_type: LogicalType::Boolean,
        })
    }

    fn make_string_constant(value: &str) -> Expression {
        Expression::Constant(ConstantExpression {
            value: Value::Varchar(value.to_string()),
            return_type: LogicalType::Varchar,
        })
    }

    #[test]
    fn test_constant_folding_matcher_excludes_constants() {
        let matcher = ConstantFoldingMatcher;
        let expr = make_constant(42);
        let mut bindings = Vec::new();

        // Pure constants should NOT match
        assert!(!matcher.matches(&expr, &mut bindings));

        // String constants should also NOT match
        let str_expr = make_string_constant("hello");
        assert!(!matcher.matches(&str_expr, &mut bindings));
    }

    #[test]
    fn test_constant_folding_matcher_matches_foldable() {
        let matcher = ConstantFoldingMatcher;

        // Comparison of two constants should match
        let comp_expr = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            make_constant(1),
            make_constant(2),
        ));

        let mut bindings = Vec::new();
        assert!(matcher.matches(&comp_expr, &mut bindings));
    }

    #[test]
    fn test_evaluate_comparison_equal() {
        let left = Value::Integer(5);
        let right = Value::Integer(5);
        let result = evaluate_comparison(&left, &right, ComparisonType::Equal);
        assert_eq!(result, Some(Value::Boolean(true)));

        let right = Value::Integer(3);
        let result = evaluate_comparison(&left, &right, ComparisonType::Equal);
        assert_eq!(result, Some(Value::Boolean(false)));
    }

    #[test]
    fn test_evaluate_comparison_less_than() {
        let left = Value::Integer(3);
        let right = Value::Integer(5);
        let result = evaluate_comparison(&left, &right, ComparisonType::LessThan);
        assert_eq!(result, Some(Value::Boolean(true)));

        let result = evaluate_comparison(&right, &left, ComparisonType::LessThan);
        assert_eq!(result, Some(Value::Boolean(false)));
    }

    #[test]
    fn test_evaluate_comparison_with_null() {
        let left = Value::Integer(5);
        let right = Value::Null(LogicalType::Integer);

        // Regular comparison with NULL returns NULL
        let result = evaluate_comparison(&left, &right, ComparisonType::Equal);
        assert!(matches!(result, Some(Value::Null(_))));

        // IS DISTINCT FROM treats NULL as a value
        let result = evaluate_comparison(&left, &right, ComparisonType::DistinctFrom);
        assert_eq!(result, Some(Value::Boolean(true)));
    }

    #[test]
    fn test_evaluate_conjunction_and() {
        let values = vec![Value::Boolean(true), Value::Boolean(true)];
        let result = evaluate_conjunction(&values, ConjunctionType::And);
        assert_eq!(result, Some(Value::Boolean(true)));

        let values = vec![Value::Boolean(true), Value::Boolean(false)];
        let result = evaluate_conjunction(&values, ConjunctionType::And);
        assert_eq!(result, Some(Value::Boolean(false)));
    }

    #[test]
    fn test_evaluate_conjunction_or() {
        let values = vec![Value::Boolean(false), Value::Boolean(false)];
        let result = evaluate_conjunction(&values, ConjunctionType::Or);
        assert_eq!(result, Some(Value::Boolean(false)));

        let values = vec![Value::Boolean(false), Value::Boolean(true)];
        let result = evaluate_conjunction(&values, ConjunctionType::Or);
        assert_eq!(result, Some(Value::Boolean(true)));
    }

    #[test]
    fn test_evaluate_add() {
        let values = vec![Value::Integer(2), Value::Integer(3)];
        let result = evaluate_add(&values, &LogicalType::Integer);
        assert_eq!(result, Some(Value::Integer(5)));
    }

    #[test]
    fn test_evaluate_subtract() {
        let values = vec![Value::Integer(5), Value::Integer(3)];
        let result = evaluate_subtract(&values, &LogicalType::Integer);
        assert_eq!(result, Some(Value::Integer(2)));
    }

    #[test]
    fn test_evaluate_multiply() {
        let values = vec![Value::Integer(4), Value::Integer(3)];
        let result = evaluate_multiply(&values, &LogicalType::Integer);
        assert_eq!(result, Some(Value::Integer(12)));
    }

    #[test]
    fn test_evaluate_divide() {
        let values = vec![Value::Integer(10), Value::Integer(2)];
        let result = evaluate_divide(&values, &LogicalType::Integer);
        assert_eq!(result, Some(Value::Integer(5)));

        // Division by zero returns None
        let values = vec![Value::Integer(10), Value::Integer(0)];
        let result = evaluate_divide(&values, &LogicalType::Integer);
        assert_eq!(result, None);
    }

    #[test]
    fn test_evaluate_concat() {
        let values = vec![
            Value::Varchar("hello".to_string()),
            Value::Varchar(" world".to_string()),
        ];
        let result = evaluate_concat(&values);
        assert_eq!(result, Some(Value::Varchar("hello world".to_string())));
    }

    #[test]
    fn test_evaluate_length() {
        let values = vec![Value::Varchar("hello".to_string())];
        let result = evaluate_length(&values);
        assert_eq!(result, Some(Value::BigInt(5)));
    }

    #[test]
    fn test_evaluate_upper_lower() {
        let values = vec![Value::Varchar("Hello".to_string())];
        let result = evaluate_upper(&values);
        assert_eq!(result, Some(Value::Varchar("HELLO".to_string())));

        let result = evaluate_lower(&values);
        assert_eq!(result, Some(Value::Varchar("hello".to_string())));
    }

    #[test]
    fn test_constant_folding_rule_comparison() {
        let rule = ConstantFoldingRule::new();

        // Create: 1 = 1
        let comp_expr = Expression::Comparison(ComparisonExpression::new(
            ComparisonType::Equal,
            make_constant(1),
            make_constant(1),
        ));

        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&comp_expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        match result {
            RuleResult::Changed(expr) => match *expr {
                Expression::Constant(c) => {
                    assert_eq!(c.value, Value::Boolean(true));
                }
                _ => panic!("Expected Constant"),
            },
            _ => panic!("Expected Changed result with boolean constant"),
        }
    }

    #[test]
    fn test_constant_folding_rule_conjunction() {
        let rule = ConstantFoldingRule::new();

        // Create: true AND false
        let conj_expr = Expression::Conjunction(ConjunctionExpression {
            conjunction_type: ConjunctionType::And,
            children: vec![make_bool_constant(true), make_bool_constant(false)],
        });

        let mut bindings = Vec::new();
        assert!(rule.matcher().matches(&conj_expr, &mut bindings));

        let dummy_op = LogicalOperator::DummyScan;
        let result = rule.apply(&dummy_op, bindings, false);

        match result {
            RuleResult::Changed(expr) => match *expr {
                Expression::Constant(c) => {
                    assert_eq!(c.value, Value::Boolean(false));
                }
                _ => panic!("Expected Constant"),
            },
            _ => panic!("Expected Changed result with boolean constant"),
        }
    }

    #[test]
    fn test_constant_folding_rule_name() {
        let rule = ConstantFoldingRule::new();
        assert_eq!(rule.name(), "ConstantFoldingRule");
    }
}
