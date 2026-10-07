// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Independent SQL 3VL and bag oracle over the actual transfer result.
//! No optimizer evaluator, simplifier, or domain inference is used by the oracle.

use super::*;
use paro_common::{runtime_value::Value, types::LogicalType};
use paro_function::aggregate::distributive::sum::get_sum_function;
use paro_function::scalar::{FunctionStability, ScalarFunction};
use paro_planner::expression::{
    AggregateExpression, ColumnRefExpression, ComparisonExpression, ComparisonType,
    ConstantExpression, FunctionExpression,
};
use paro_planner::{logical::operator::ColumnBinding, logical::plan::OwnedLogicalPlan};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Cell {
    Null,
    Int(i64),
    Text(String),
    Bool(bool),
}

type Row = BTreeMap<(usize, usize), Cell>;
type Bag = BTreeMap<Row, usize>;

fn truth(value: Cell) -> Option<bool> {
    match value {
        Cell::Null => None,
        Cell::Bool(value) => Some(value),
        other => panic!("non-boolean predicate: {other:?}"),
    }
}

fn boolean(value: Option<bool>) -> Cell {
    value.map(Cell::Bool).unwrap_or(Cell::Null)
}

fn eval(expression: &Expression, row: &Row) -> Cell {
    match expression {
        Expression::ColumnRef(column) => {
            assert_eq!(column.depth, 0);
            row.get(&(column.binding.table_index, column.binding.column_index))
                .expect("oracle encountered a foreign binding")
                .clone()
        }
        Expression::Constant(constant) => match &constant.value {
            Value::Null(_) => Cell::Null,
            Value::Integer(value) => Cell::Int(i64::from(*value)),
            Value::BigInt(value) => Cell::Int(*value),
            Value::Varchar(value) => Cell::Text(value.clone()),
            Value::Boolean(value) => Cell::Bool(*value),
            other => panic!("unsupported oracle constant: {other:?}"),
        },
        Expression::Comparison(comparison) => {
            let left = eval(&comparison.left, row);
            let right = eval(&comparison.right, row);
            let null = left == Cell::Null || right == Cell::Null;
            use ComparisonType::*;
            Cell::Bool(match comparison.comparison_type {
                DistinctFrom => left != right,
                NotDistinctFrom => left == right,
                _ if null => return Cell::Null,
                Equal => left == right,
                NotEqual => left != right,
                LessThan => left < right,
                LessThanOrEqual => left <= right,
                GreaterThan => left > right,
                GreaterThanOrEqual => left >= right,
            })
        }
        Expression::Conjunction(conjunction) => {
            let is_and = conjunction.conjunction_type == ConjunctionType::And;
            let mut unknown = false;
            for child in &conjunction.children {
                match truth(eval(child, row)) {
                    Some(value) if value != is_and => return Cell::Bool(value),
                    None => unknown = true,
                    _ => {}
                }
            }
            boolean((!unknown).then_some(is_and))
        }
        other => panic!("unsupported actual transfer expression: {other:?}"),
    }
}

fn accepts(predicates: &[Expression], row: &Row) -> bool {
    predicates
        .iter()
        .all(|predicate| truth(eval(predicate, row)) == Some(true))
}

fn bag(rows: impl IntoIterator<Item = Row>) -> Bag {
    let mut bag = Bag::new();
    for row in rows {
        *bag.entry(row).or_default() += 1;
    }
    bag
}

fn col(table: usize, index: usize, ty: LogicalType) -> Expression {
    Expression::ColumnRef(ColumnRefExpression::new(ColumnBinding::new(table, index), ty).into())
}

fn int(value: i32) -> Expression {
    Expression::Constant(
        ConstantExpression::new(Value::Integer(value), LogicalType::Integer).into(),
    )
}

fn text(value: &str) -> Expression {
    Expression::Constant(
        ConstantExpression::new(Value::Varchar(value.into()), LogicalType::Varchar).into(),
    )
}

fn cmp(kind: ComparisonType, left: Expression, right: Expression) -> Expression {
    Expression::Comparison(ComparisonExpression::new(kind, left, right).into())
}

fn eq(left: Expression, right: Expression) -> Expression {
    cmp(ComparisonType::Equal, left, right)
}

fn and(left: Expression, right: Expression) -> Expression {
    Expression::Conjunction(
        ConjunctionExpression::new(ConjunctionType::And, vec![left, right]).into(),
    )
}

fn or(left: Expression, right: Expression) -> Expression {
    Expression::Conjunction(
        ConjunctionExpression::new(ConjunctionType::Or, vec![left, right]).into(),
    )
}

fn input_layout() -> LogicalOutputLayout {
    LogicalOutputLayout::new(
        vec![LogicalType::Integer; 2],
        vec![ColumnBinding::new(10, 0), ColumnBinding::new(10, 1)],
    )
}

fn dummy() -> OwnedLogicalPlan {
    OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan)
}

fn input(amount: Option<i64>, key: Option<i64>) -> Row {
    BTreeMap::from([
        ((10, 0), amount.map(Cell::Int).unwrap_or(Cell::Null)),
        ((10, 1), key.map(Cell::Int).unwrap_or(Cell::Null)),
    ])
}

fn project(projection: &Projection, row: &Row) -> Row {
    projection
        .expressions
        .iter()
        .enumerate()
        .map(|(index, expression)| ((projection.table_index, index), eval(expression, row)))
        .collect()
}

#[test]
fn projection_channel_constants_and_cross_ordinal_renames_preserve_3vl_bags() {
    let layout = input_layout();
    // y comes from ordinal 1, amount from ordinal 0: ordinal-only rebinding is wrong.
    let channel = col(20, 2, LogicalType::Varchar);
    let y = col(20, 0, LogicalType::Integer);
    let amount = col(20, 1, LogicalType::Integer);
    let mut rows = Vec::new();
    for key in [None, Some(1), Some(2), Some(3)] {
        for amount in [None, Some(-2), Some(0), Some(1), Some(2)] {
            rows.extend([input(amount, key), input(amount, key)]);
        }
    }
    for channel_value in [Some("web"), Some("store"), None] {
        let constant = channel_value.map(text).unwrap_or_else(|| {
            Expression::Constant(
                ConstantExpression::new(Value::Null(LogicalType::Varchar), LogicalType::Varchar)
                    .into(),
            )
        });
        let projection = Projection::new(
            20,
            dummy(),
            vec![
                col(10, 1, LogicalType::Integer),
                col(10, 0, LogicalType::Integer),
                constant,
            ],
        );
        let operator = LogicalOperator::Projection(projection);
        let LogicalOperator::Projection(projection) = &operator else {
            unreachable!()
        };
        let predicates = [
            eq(channel.clone(), text("web")),
            and(eq(channel.clone(), text("web")), eq(y.clone(), int(1))),
            or(
                and(eq(channel.clone(), text("web")), eq(y.clone(), int(1))),
                and(
                    eq(channel.clone(), text("store")),
                    eq(amount.clone(), int(2)),
                ),
            ),
            eq(y.clone(), amount.clone()),
        ];
        for predicate in predicates {
            let transfer =
                transfer_predicates(&operator, &[&layout], &[predicate.clone()]).unwrap();
            assert!(transfer.has_moved());
            assert!(transfer.remaining.is_empty());
            assert_eq!(transfer.child_predicates.len(), 1);
            for row in &rows {
                let original = truth(eval(&predicate, &project(projection, row)));
                if original == Some(true) {
                    assert!(accepts(&transfer.child_predicates[0], row));
                }
            }
            let expected = bag(rows
                .iter()
                .map(|row| project(projection, row))
                .filter(|row| accepts(&[predicate.clone()], row)));
            let actual = bag(rows
                .iter()
                .filter(|row| accepts(&transfer.child_predicates[0], row))
                .map(|row| project(projection, row))
                .filter(|row| accepts(&transfer.remaining, row)));
            assert_eq!(
                actual, expected,
                "channel={channel_value:?}, predicate={predicate:?}"
            );
        }
        // The branch constant must actually be folded, not merely substituted.
        let transfer =
            transfer_predicates(&operator, &[&layout], &[eq(channel.clone(), text("web"))])
                .unwrap();
        assert!(matches!(
            transfer.child_predicates[0].as_ref(),
            [Expression::Constant(_)]
        ));
    }
}

fn aggregate_operator() -> LogicalOperator {
    let (sum, _) = get_sum_function().bind(&[LogicalType::Integer]).unwrap();
    let total = Expression::Aggregate(
        AggregateExpression::new(
            sum,
            vec![col(10, 0, LogicalType::Integer)],
            LogicalType::BigInt,
        )
        .into(),
    );
    LogicalOperator::Aggregate(
        Aggregate::new(
            30,
            31,
            32,
            dummy(),
            vec![col(10, 1, LogicalType::Integer)],
            vec![],
            vec![total],
            vec![],
        )
        .into(),
    )
}

// Independent GROUP BY y / SUM(amount). NULL keys group together, NULL amounts
// do not contribute, all-NULL sums remain NULL, and duplicate rows contribute.
fn aggregate(rows: impl IntoIterator<Item = Row>) -> Vec<Row> {
    let mut groups: BTreeMap<Cell, Option<i64>> = BTreeMap::new();
    for row in rows {
        let total = groups.entry(row[&(10, 1)].clone()).or_default();
        if let Cell::Int(amount) = row[&(10, 0)] {
            *total = Some(total.unwrap_or(0) + amount);
        }
    }
    groups
        .into_iter()
        .map(|(key, total)| {
            BTreeMap::from([
                ((30, 0), key),
                ((31, 0), total.map(Cell::Int).unwrap_or(Cell::Null)),
            ])
        })
        .collect()
}

#[test]
fn aggregate_necessary_domains_and_remaining_match_exhaustive_nullable_bags() {
    let operator = aggregate_operator();
    let layout = input_layout();
    let a = eq(col(30, 0, LogicalType::Integer), int(1));
    let b = eq(col(30, 0, LogicalType::Integer), int(2));
    let positive = cmp(
        ComparisonType::GreaterThan,
        col(31, 0, LogicalType::BigInt),
        Expression::Constant(ConstantExpression::new(Value::BigInt(0), LogicalType::BigInt).into()),
    );
    let cases = [
        (
            or(a.clone(), and(b.clone(), positive.clone())),
            vec![1, 2],
            true,
        ),
        (and(b.clone(), positive.clone()), vec![2], true),
        (or(a.clone(), positive.clone()), vec![], true),
        (
            and(or(a.clone(), positive.clone()), b.clone()),
            vec![2],
            true,
        ),
        (or(a, b), vec![1, 2], false),
    ];
    let atoms: Vec<_> = [None, Some(1), Some(2), Some(3)]
        .into_iter()
        .flat_map(|key| {
            [None, Some(-2), Some(0), Some(1)]
                .into_iter()
                .map(move |amount| input(amount, key))
        })
        .collect();
    for (predicate, admitted_keys, has_remaining) in cases {
        let transfer = transfer_predicates(&operator, &[&layout], &[predicate.clone()]).unwrap();
        assert_eq!(transfer.child_predicates.len(), 1);
        assert_eq!(transfer.has_moved(), !admitted_keys.is_empty());
        assert_eq!(transfer.remaining.len(), usize::from(has_remaining));
        // Check the concrete necessary domain, so a no-op implementation cannot pass.
        for row in &atoms {
            let expected = admitted_keys.is_empty()
                || matches!(row[&(10, 1)], Cell::Int(key) if admitted_keys.contains(&key));
            assert_eq!(accepts(&transfer.child_predicates[0], row), expected);
        }
        // Exhaust all ordered bags of length <= 3, including duplicates, empty
        // input, all-NULL groups, zero/negative totals and mixed-sign sums.
        for length in 0..=3 {
            for mut code in 0..atoms.len().pow(length) {
                let mut rows = Vec::new();
                for _ in 0..length {
                    rows.push(atoms[code % atoms.len()].clone());
                    code /= atoms.len();
                }
                let original_groups = aggregate(rows.clone());
                for group in &original_groups {
                    if accepts(&[predicate.clone()], group) {
                        for row in rows.iter().filter(|row| row[&(10, 1)] == group[&(30, 0)]) {
                            assert!(
                                accepts(&transfer.child_predicates[0], row),
                                "TRUE implication failed: {predicate:?}, {rows:?}"
                            );
                        }
                    }
                    if has_remaining {
                        assert_eq!(
                            eval(&transfer.remaining[0], group),
                            eval(&predicate, group),
                            "remaining must preserve original 3VL"
                        );
                    }
                }
                let expected = bag(original_groups
                    .into_iter()
                    .filter(|row| accepts(&[predicate.clone()], row)));
                let actual = bag(aggregate(
                    rows.iter()
                        .filter(|row| accepts(&transfer.child_predicates[0], row))
                        .cloned(),
                )
                .into_iter()
                .filter(|row| accepts(&transfer.remaining, row)));
                assert_eq!(actual, expected, "predicate={predicate:?}, input={rows:?}");
            }
        }
    }
}

#[test]
fn projection_evaluation_fence_retains_original_predicate() {
    let volatile = Expression::Function(
        FunctionExpression::new(
            ScalarFunction::new(
                "domain_oracle_volatile".into(),
                vec![],
                LogicalType::Integer,
                |_, _, _| Ok(()),
            )
            .with_stability(FunctionStability::Volatile),
            vec![],
            LogicalType::Integer,
        )
        .into(),
    );
    assert!(volatile.evaluation_properties().is_reorder_fence());
    let operator = LogicalOperator::Projection(Projection::new(
        20,
        dummy(),
        vec![col(10, 1, LogicalType::Integer), volatile],
    ));
    let predicate = eq(col(20, 0, LogicalType::Integer), int(1));
    let layout = input_layout();
    let transfer = transfer_predicates(&operator, &[&layout], &[predicate.clone()]).unwrap();
    assert!(!transfer.has_moved());
    assert_eq!(transfer.remaining.len(), 1);
    // Even a predicate on the other, plain column cannot cross the fence.
    for key in [Cell::Null, Cell::Int(1), Cell::Int(2)] {
        let row = BTreeMap::from([((20, 0), key)]);
        assert_eq!(eval(&transfer.remaining[0], &row), eval(&predicate, &row));
    }
}

#[test]
fn foreign_correlated_and_out_of_range_namespaces_are_rejected() {
    let layout = input_layout();
    let projection = LogicalOperator::Projection(Projection::new(
        20,
        dummy(),
        vec![col(10, 1, LogicalType::Integer)],
    ));
    for (operator, table) in [(projection, 20), (aggregate_operator(), 30)] {
        for bad in [
            col(99, 0, LogicalType::Integer),
            col(table, 9, LogicalType::Integer),
            Expression::ColumnRef(
                ColumnRefExpression::with_depth(
                    ColumnBinding::new(table, 0),
                    LogicalType::Integer,
                    1,
                )
                .into(),
            ),
        ] {
            let predicate = and(
                eq(col(table, 0, LogicalType::Integer), int(1)),
                eq(bad, int(2)),
            );
            assert!(transfer_predicates(&operator, &[&layout], &[predicate]).is_none());
        }
    }
    let projection = LogicalOperator::Projection(Projection::new(
        20,
        dummy(),
        vec![col(99, 1, LogicalType::Integer)],
    ));
    let predicate = eq(col(20, 0, LogicalType::Integer), int(1));
    let transfer = transfer_predicates(&projection, &[&layout], &[predicate]).unwrap();
    assert!(!transfer.has_moved());
    assert_eq!(transfer.remaining.len(), 1);
}
