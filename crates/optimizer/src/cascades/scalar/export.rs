// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! The native scalar DAG's explicit execution boundary. No original expression
//! payload is required; bound kernels and child roles are owned by the DAG.

use std::collections::HashMap;

use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_planner::expression::{
    CaseExpression, CastExpression, ComparisonExpression, ComparisonType, ConjunctionExpression,
    ConjunctionType, Expression, OperatorExpression,
};

use super::super::ids::{ColumnId, ScalarExprId};
use super::{ComparisonOp, ScalarArena, ScalarKind, ScalarNode};

impl ScalarArena {
    /// Reconstruct only the reachable scalar DAG at an execution boundary.
    /// Column/parameter slots are supplied by the selected physical context,
    /// never inferred from an old extraction tree. Check admission/cancellation
    /// before allocating each distinct node, including zero-argument calls.
    pub fn export_expression(
        &self,
        root: ScalarExprId,
        mut column: impl FnMut(ColumnId, &LogicalType, usize) -> Result<Expression>,
        mut parameter: impl FnMut(u32, &LogicalType) -> Result<Expression>,
        mut checkpoint: impl FnMut() -> Result<()>,
    ) -> Result<Expression> {
        // A cursor retains one suspended parent, not a vector of all its
        // unadmitted siblings. A very wide IN/ARRAY node must not allocate a
        // width-sized traversal frontier before its next cancellation check.
        let mut pending = vec![(root, 0)];
        let mut completed = HashMap::<ScalarExprId, Expression>::new();
        while let Some((id, cursor)) = pending.pop() {
            if completed.contains_key(&id) {
                continue;
            }
            let node = self
                .get(id)
                .ok_or_else(|| paro_error::internal("scalar export references an unknown node"))?;
            if cursor == 0 {
                checkpoint()?;
            }
            if let Some(child) = node.children.get(cursor) {
                pending.push((id, cursor + 1));
                pending.push((*child, 0));
                continue;
            }
            let children =
                node.children
                    .iter()
                    .map(|id| {
                        completed.get(id).cloned().ok_or_else(|| {
                            paro_error::internal("scalar export lost a completed child")
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
            let expression = match &node.kind {
                ScalarKind::Column(id) => column(*id, &node.logical_type, 0)?,
                ScalarKind::CorrelatedColumn { column: id, depth } => {
                    column(*id, &node.logical_type, *depth)?
                }
                ScalarKind::Parameter(slot) => parameter(*slot, &node.logical_type)?,
                _ => export_local(node, children)?,
            };
            if expression.return_type() != node.logical_type {
                return Err(paro_error::internal(
                    "scalar export changed its declared type",
                ));
            }
            completed.insert(id, expression);
        }
        completed
            .remove(&root)
            .ok_or_else(|| paro_error::internal("scalar export has no root"))
    }
}

fn export_local(node: &ScalarNode, children: Vec<Expression>) -> Result<Expression> {
    Ok(match &node.kind {
        ScalarKind::Constant { value } => value.expression(),
        ScalarKind::Function { routine } => {
            Expression::Function(routine.instantiate(children).into())
        }
        ScalarKind::Aggregate { function } => {
            Expression::Aggregate(function.instantiate(children)?.into())
        }
        ScalarKind::Window { function } => {
            Expression::Window(function.instantiate(children)?.into())
        }
        ScalarKind::Operator { operator } => Expression::Operator(
            OperatorExpression::new(*operator, children, node.logical_type.clone()).into(),
        ),
        ScalarKind::And | ScalarKind::Or => Expression::Conjunction(
            ConjunctionExpression::new(
                if node.kind == ScalarKind::And {
                    ConjunctionType::And
                } else {
                    ConjunctionType::Or
                },
                children,
            )
            .into(),
        ),
        ScalarKind::Cast { try_cast, binding } => {
            let [child]: [Expression; 1] = children
                .try_into()
                .map_err(|_| paro_error::internal("native cast has invalid arity"))?;
            Expression::Cast(
                CastExpression::new(
                    child,
                    node.logical_type.clone(),
                    binding.binding().clone(),
                    *try_cast,
                )
                .into(),
            )
        }
        ScalarKind::Case => {
            let [check, yes, no]: [Expression; 3] = children
                .try_into()
                .map_err(|_| paro_error::internal("native CASE has invalid arity"))?;
            Expression::Case(CaseExpression::new(check, yes, no, node.logical_type.clone()).into())
        }
        ScalarKind::Comparison(op) => {
            let [left, right]: [Expression; 2] = children
                .try_into()
                .map_err(|_| paro_error::internal("native comparison has invalid arity"))?;
            let op = match op {
                ComparisonOp::Equal => ComparisonType::Equal,
                ComparisonOp::NotEqual => ComparisonType::NotEqual,
                ComparisonOp::Less => ComparisonType::LessThan,
                ComparisonOp::LessOrEqual => ComparisonType::LessThanOrEqual,
                ComparisonOp::Greater => ComparisonType::GreaterThan,
                ComparisonOp::GreaterOrEqual => ComparisonType::GreaterThanOrEqual,
                ComparisonOp::DistinctFrom => ComparisonType::DistinctFrom,
                ComparisonOp::NotDistinctFrom => ComparisonType::NotDistinctFrom,
            };
            Expression::Comparison(ComparisonExpression::new(op, left, right).into())
        }
        ScalarKind::Column(_) | ScalarKind::CorrelatedColumn { .. } | ScalarKind::Parameter(_) => {
            return Err(paro_error::internal(
                "contextual scalar operand was not resolved",
            ))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cascades::column::ColumnCatalog;
    use crate::cascades::scalar_lowering::{intern_expression, BindingCatalog};
    use paro_common::runtime_value::Value;
    use paro_common::typed_parameters::{ParameterSlot, RuntimeParamId};
    use paro_function::aggregate::distributive::count::get_count_star_function;
    use paro_function::scalar::cast::BoundCastInfo;
    use paro_function::scalar::{BoundScalarFunction, FunctionData, ScalarFunction as Kernel};
    use paro_function::window::{WindowFunction, WindowFunctionType};
    use paro_planner::expression::{
        AggregateExpression, AggregateType, ColumnRefExpression, ConstantExpression,
        FunctionExpression, OperatorType, OrderByExpression, ParameterExpression, WindowExpression,
        WindowFrame, WindowFrameBound, WindowFrameType,
    };
    use paro_planner::operator::ColumnBinding;

    fn integer(value: i32) -> Expression {
        Expression::Constant(
            ConstantExpression::new(Value::Integer(value), LogicalType::Integer).into(),
        )
    }
    fn boolean(value: bool) -> Expression {
        Expression::Constant(
            ConstantExpression::new(Value::Boolean(value), LogicalType::Boolean).into(),
        )
    }
    fn lower(expression: &Expression, arena: &mut ScalarArena) -> ScalarExprId {
        intern_expression(
            expression,
            &[],
            &mut BindingCatalog::default(),
            &mut ColumnCatalog::default(),
            arena,
        )
        .unwrap()
    }
    fn export(arena: &ScalarArena, root: ScalarExprId) -> Expression {
        arena
            .export_expression(
                root,
                |id, ty, depth| {
                    Ok(Expression::ColumnRef(
                        ColumnRefExpression::with_depth(
                            ColumnBinding::new(0, id.index()),
                            ty.clone(),
                            depth,
                        )
                        .into(),
                    ))
                },
                |slot, ty| {
                    Ok(Expression::Parameter(
                        ParameterExpression::new(ParameterSlot::new(
                            RuntimeParamId::new(slot as usize),
                            ty.clone(),
                        ))
                        .into(),
                    ))
                },
                || Ok(()),
            )
            .unwrap()
    }
    fn roundtrip(expression: Expression) -> (ScalarArena, ScalarExprId) {
        let mut arena = ScalarArena::default();
        let root = lower(&expression, &mut arena);
        let expected = expression.clone();
        drop(expression);
        assert!(expected.equals(&export(&arena, root)), "{expected:?}");
        (arena, root)
    }

    #[derive(Debug, Clone)]
    struct CollisionData(u64);
    impl FunctionData for CollisionData {
        fn clone_box(&self) -> Box<dyn FunctionData> {
            Box::new(self.clone())
        }
        fn equals(&self, other: &dyn FunctionData) -> bool {
            other
                .as_any()
                .downcast_ref::<Self>()
                .is_some_and(|other| self.0 == other.0)
        }
        fn fingerprint(&self) -> u64 {
            42
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }
    fn function(value: u64) -> Expression {
        let kernel = Kernel::new(
            "native_call_test".into(),
            vec![LogicalType::Integer],
            LogicalType::Integer,
            |_, _, _| Ok(()),
        );
        Expression::Function(
            FunctionExpression::new(
                BoundScalarFunction::from(kernel).with_bind_data(CollisionData(value)),
                vec![integer(2)],
                LogicalType::Integer,
            )
            .into(),
        )
    }

    #[test]
    fn executable_call_bind_data_is_owned_and_digest_collision_is_not_equality() {
        let mut arena = ScalarArena::default();
        let first = lower(&function(1), &mut arena);
        let second = lower(&function(2), &mut arena);
        assert_ne!(first, second);
        assert_eq!(
            arena.get(first).unwrap().fingerprint,
            arena.get(second).unwrap().fingerprint
        );
        assert_eq!(first, lower(&function(1), &mut arena));
        for (root, expected) in [(first, 1), (second, 2)] {
            let Expression::Function(expression) = export(&arena, root) else {
                panic!()
            };
            assert_eq!(
                expression
                    .function
                    .get_bind_data::<CollisionData>()
                    .unwrap()
                    .0,
                expected
            );
        }
    }

    #[test]
    fn native_call_construction_cannot_weaken_its_executable_effect_contract() {
        use crate::cascades::scalar::{ScalarFunction, ScalarSpec, Volatility};
        let mut kernel: BoundScalarFunction = Kernel::new(
            "native_volatile".into(),
            vec![],
            LogicalType::Integer,
            |_, _, _| Ok(()),
        )
        .into();
        kernel.stability = paro_function::scalar::FunctionStability::Volatile;
        let mut arena = ScalarArena::default();
        let root = arena
            .intern(ScalarSpec {
                kind: ScalarKind::Function {
                    routine: ScalarFunction::new(kernel, LogicalType::Integer, None),
                },
                logical_type: LogicalType::Integer,
                children: Box::new([]),
                local_properties: Default::default(),
            })
            .unwrap();
        let node = arena.get(root).unwrap();
        assert_eq!(node.properties.volatility, Volatility::Volatile);
        assert!(node.properties.may_error);
        assert!(!node.properties.can_reorder_and_share());
        assert!(!node.properties.can_repeat_evaluation());
    }

    #[test]
    fn native_scalars_export_without_an_owned_extraction_template() {
        roundtrip(function(7));
        roundtrip(Expression::Operator(
            OperatorExpression::new(
                OperatorType::Coalesce,
                vec![integer(3), integer(4)],
                LogicalType::Integer,
            )
            .into(),
        ));
        roundtrip(Expression::Case(
            CaseExpression::new(boolean(true), integer(3), integer(4), LogicalType::Integer).into(),
        ));
        roundtrip(Expression::ColumnRef(
            ColumnRefExpression::with_depth(ColumnBinding::new(0, 0), LogicalType::Integer, 3)
                .into(),
        ));
        roundtrip(Expression::Parameter(
            ParameterExpression::new(ParameterSlot::new(
                RuntimeParamId::new(7),
                LogicalType::Integer,
            ))
            .into(),
        ));
        let binding = BoundCastInfo::fixed(|_, _, _, _| Ok(true)).requiring_runtime_context();
        let cast = Expression::Cast(
            CastExpression::new(integer(2), LogicalType::BigInt, binding.clone(), true).into(),
        );
        let (arena, root) = roundtrip(cast);
        let Expression::Cast(exported) = export(&arena, root) else {
            panic!()
        };
        assert!(binding.execution_semantics_equal(&exported.cast_info));
        assert!(exported.try_cast);
    }

    fn aggregate(value: u64) -> AggregateExpression {
        AggregateExpression::new(get_count_star_function(), vec![], LogicalType::BigInt)
            .with_filter(Some(boolean(true)))
            .with_order_bys(vec![OrderByExpression {
                expression: integer(17),
                ascending: false,
                nulls_first: true,
            }])
            .with_bind_info(Some(std::sync::Arc::new(CollisionData(value))))
    }

    #[test]
    fn aggregate_modifiers_and_colliding_bind_data_remain_distinct() {
        let mut arena = ScalarArena::default();
        let first = lower(&Expression::Aggregate(aggregate(1).into()), &mut arena);
        let second = lower(&Expression::Aggregate(aggregate(2).into()), &mut arena);
        assert_ne!(first, second);
        assert_eq!(
            arena.get(first).unwrap().fingerprint,
            arena.get(second).unwrap().fingerprint
        );
        roundtrip(Expression::Aggregate(
            aggregate(1).with_aggr_type(AggregateType::Distinct).into(),
        ));
        let Expression::Aggregate(result) = export(&arena, second) else {
            panic!()
        };
        assert!(result
            .function
            .execution_semantics_equal(&get_count_star_function()));
        assert_eq!(
            result
                .bind_info
                .as_ref()
                .unwrap()
                .as_any()
                .downcast_ref::<CollisionData>()
                .unwrap()
                .0,
            2
        );
        assert!(result.filter.as_ref().unwrap().equals(&boolean(true)));
        assert!(result.order_bys[0].expression.equals(&integer(17)));
    }

    #[test]
    fn window_argument_roles_and_frame_kinds_survive_roundtrip() {
        let native = WindowFunction::new(
            "row_number",
            WindowFunctionType::RowNumber,
            vec![],
            LogicalType::BigInt,
        );
        let frames = [
            WindowFrameBound::Unbounded,
            WindowFrameBound::CurrentRow,
            WindowFrameBound::Offset(Box::new(integer(2))),
        ];
        for start in &frames {
            for end in &frames {
                let frame = WindowFrame {
                    frame_type: WindowFrameType::Rows,
                    start_bound: start.clone(),
                    start_is_preceding: true,
                    end_bound: end.clone(),
                    end_is_preceding: false,
                };
                let order = OrderByExpression {
                    expression: integer(11),
                    ascending: true,
                    nulls_first: false,
                };
                roundtrip(Expression::Window(
                    WindowExpression::native(
                        native.clone(),
                        vec![],
                        vec![integer(13)],
                        vec![order.clone()],
                        frame.clone(),
                        false,
                    )
                    .into(),
                ));
                roundtrip(Expression::Window(
                    WindowExpression::aggregate(
                        aggregate(1),
                        vec![integer(13)],
                        vec![order],
                        frame,
                    )
                    .into(),
                ));
            }
        }
        let mut arena = ScalarArena::default();
        let mut window = WindowExpression::native(
            native,
            vec![],
            vec![],
            vec![],
            WindowFrame::default(),
            false,
        );
        let first = lower(&Expression::Window(window.clone().into()), &mut arena);
        window.frame.start_bound = WindowFrameBound::CurrentRow;
        let second = lower(&Expression::Window(window.into()), &mut arena);
        assert_ne!(first, second);
        assert_ne!(
            arena.get(first).unwrap().fingerprint,
            arena.get(second).unwrap().fingerprint
        );
    }

    #[test]
    fn scalar_export_is_stack_safe_preserves_dag_sharing_and_checks_before_descent() {
        std::thread::Builder::new()
            .stack_size(256 * 1024)
            .spawn(|| {
                let leaf = integer(1);
                let mut expression = leaf.clone();
                for _ in 0..10_000 {
                    expression = Expression::Case(
                        CaseExpression::new(
                            boolean(true),
                            expression.clone(),
                            expression,
                            LogicalType::Integer,
                        )
                        .into(),
                    );
                }
                let mut arena = ScalarArena::default();
                let root = lower(&expression, &mut arena);
                drop(expression);
                let mut checkpoints = 0;
                let exported = arena
                    .export_expression(
                        root,
                        |_, _, _| unreachable!(),
                        |_, _| unreachable!(),
                        || {
                            checkpoints += 1;
                            Ok(())
                        },
                    )
                    .unwrap();
                assert_eq!(checkpoints, 10_002);
                let Expression::Case(case) = &exported else {
                    panic!()
                };
                assert_eq!(
                    case.result_if_true.allocation_identity(),
                    case.result_if_false.allocation_identity()
                );
                let mut attempts = 0;
                let error = arena
                    .export_expression(
                        root,
                        |_, _, _| unreachable!(),
                        |_, _| unreachable!(),
                        || {
                            attempts += 1;
                            Err(paro_error::internal("test admission rejected"))
                        },
                    )
                    .unwrap_err();
                assert!(error.to_string().contains("test admission rejected"));
                assert_eq!(attempts, 1);
                drop(exported);
            })
            .unwrap()
            .join()
            .unwrap();
    }
}
