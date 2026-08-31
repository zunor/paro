//! Canonical lowering from bound scalar expressions to optimizer-owned IR.
//!
//! The boundary is deliberately one-way: relational Memo expressions keep only
//! `ScalarExprId`s, while executable expression trees remain in extraction
//! payloads and never participate in Memo identity.

use std::collections::BTreeMap;

use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_external::routine::identity::{
    BuiltinIntrinsicId, BuiltinSemanticTag, RoutineCallIdentity,
};
use paro_function::scalar::{FunctionErrorMode, FunctionSideEffects, FunctionStability};
use paro_planner::expression::{
    ComparisonType, ConjunctionType, Expression, ExpressionIterator, WindowInvocation,
};
use paro_planner::operator::join::{Join, JoinComparisonType};
use paro_planner::operator::{ColumnBinding, LogicalOperator};
use paro_planner::visitor::enumerate_expressions;

use super::column::{ColumnCatalog, ColumnOrigin, ColumnVisibility};
use super::ids::{ColumnId, Fingerprint, ScalarExprId, StableFingerprintBuilder};
use super::scalar::{
    ComparisonOp, ScalarArena, ScalarKind, ScalarLocalProperties, ScalarSpec, Volatility,
};

type BindingMap = BTreeMap<(usize, usize, Fingerprint), ColumnId>;

pub(crate) fn intern_operator_scalars(
    operator: &mut LogicalOperator,
    output_columns: &[ColumnId],
    child_columns: &[Box<[ColumnId]>],
    binding_ids: &mut BindingMap,
    columns: &mut ColumnCatalog,
    arena: &mut ScalarArena,
) -> Result<Box<[ScalarExprId]>> {
    let default_references = match &*operator {
        LogicalOperator::SearchScan(search) => {
            get_reference_columns(&search.get, binding_ids, columns)?
        }
        LogicalOperator::FullTextFilterScan(search) => {
            get_reference_columns(&search.get, binding_ids, columns)?
        }
        _ if child_columns.is_empty() => output_columns.to_vec(),
        _ => child_columns
            .iter()
            .flat_map(|columns| columns.iter().copied())
            .collect(),
    };
    let mut roots = Vec::new();

    match operator {
        LogicalOperator::Get(get) => {
            for expression in &get.runtime_filter_expressions {
                roots.push(intern_expression(
                    expression,
                    output_columns,
                    binding_ids,
                    columns,
                    arena,
                )?);
            }
        }
        LogicalOperator::Join(Join::Comparison(join)) => {
            let left_columns = child_columns.first().map(Box::as_ref).unwrap_or(&[]);
            let right_columns = child_columns.get(1).map(Box::as_ref).unwrap_or(&[]);
            for condition in &join.conditions {
                let left =
                    intern_expression(&condition.left, left_columns, binding_ids, columns, arena)?;
                let right = intern_expression(
                    &condition.right,
                    right_columns,
                    binding_ids,
                    columns,
                    arena,
                )?;
                roots.push(intern_comparison(
                    join_comparison(condition.comparison),
                    left,
                    right,
                    arena,
                )?);
            }
            for expression in &join.duplicate_eliminated_columns {
                roots.push(intern_expression(
                    expression,
                    left_columns,
                    binding_ids,
                    columns,
                    arena,
                )?);
            }
        }
        LogicalOperator::GraphScan(scan) => {
            if let Some(expression) = &scan.filter {
                roots.push(intern_expression(
                    expression,
                    output_columns,
                    binding_ids,
                    columns,
                    arena,
                )?);
            }
        }
        LogicalOperator::GraphExpand(expand) => {
            for expression in [&expand.edge_filter, &expand.target_filter]
                .into_iter()
                .flatten()
            {
                roots.push(intern_expression(
                    expression,
                    &default_references,
                    binding_ids,
                    columns,
                    arena,
                )?);
            }
        }
        LogicalOperator::GraphMatch(graph) => {
            for element in &graph.bound_pattern.elements {
                let filter = match element {
                    paro_planner::binder::bind::graph::BoundPatternElement::Vertex(vertex) => {
                        vertex.filter.as_ref()
                    }
                    paro_planner::binder::bind::graph::BoundPatternElement::Edge(edge) => {
                        edge.filter.as_ref()
                    }
                };
                if let Some(filter) = filter {
                    roots.push(intern_expression(
                        filter,
                        &default_references,
                        binding_ids,
                        columns,
                        arena,
                    )?);
                }
            }
            for column in &graph.columns {
                roots.push(intern_expression(
                    &column.expr,
                    &default_references,
                    binding_ids,
                    columns,
                    arena,
                )?);
            }
        }
        LogicalOperator::Window(window) => {
            for expression in &window.expressions {
                roots.push(intern_expression(
                    &Expression::Window(expression.clone()),
                    &default_references,
                    binding_ids,
                    columns,
                    arena,
                )?);
            }
        }
        _ => {
            let mut error = None;
            enumerate_expressions(operator, |expression| {
                if error.is_some() {
                    return;
                }
                match intern_expression(
                    expression,
                    &default_references,
                    binding_ids,
                    columns,
                    arena,
                ) {
                    Ok(root) => roots.push(root),
                    Err(failure) => error = Some(failure),
                }
            });
            if let Some(error) = error {
                return Err(error);
            }
        }
    }
    Ok(roots.into_boxed_slice())
}

fn get_reference_columns(
    get: &paro_planner::operator::Get,
    binding_ids: &mut BindingMap,
    columns: &mut ColumnCatalog,
) -> Result<Vec<ColumnId>> {
    get.returned_types
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, logical_type)| {
            intern_column_binding(
                ColumnBinding::new(get.table_index, index),
                logical_type,
                binding_ids,
                columns,
            )
        })
        .collect()
}

fn intern_expression(
    expression: &Expression,
    reference_columns: &[ColumnId],
    binding_ids: &mut BindingMap,
    columns: &mut ColumnCatalog,
    arena: &mut ScalarArena,
) -> Result<ScalarExprId> {
    let mut children = Vec::new();
    ExpressionIterator::enumerate_children(expression, |child| children.push(child));
    let children = children
        .into_iter()
        .map(|child| intern_expression(child, reference_columns, binding_ids, columns, arena))
        .collect::<Result<Vec<_>>>()?;

    match expression {
        Expression::ColumnRef(column) => {
            let column_id = intern_column_binding(
                column.binding,
                column.return_type.clone(),
                binding_ids,
                columns,
            )?;
            arena.intern(ScalarSpec {
                kind: ScalarKind::Column(column_id),
                logical_type: column.return_type.clone(),
                children: Box::new([]),
                local_properties: ScalarLocalProperties::default(),
            })
        }
        Expression::Reference(reference) => {
            let column_id = reference_columns
                .get(reference.index)
                .copied()
                .ok_or_else(|| {
                    paro_error::internal(format!(
                        "Query IR scalar reference {} is outside its {}-column input",
                        reference.index,
                        reference_columns.len()
                    ))
                })?;
            arena.intern(ScalarSpec {
                kind: ScalarKind::Column(column_id),
                logical_type: reference.return_type.clone(),
                children: Box::new([]),
                local_properties: ScalarLocalProperties::default(),
            })
        }
        Expression::Constant(constant) => arena.intern(ScalarSpec {
            kind: ScalarKind::Constant {
                value: value_fingerprint(&constant.value),
            },
            logical_type: constant.return_type.clone(),
            children: Box::new([]),
            local_properties: ScalarLocalProperties::default(),
        }),
        Expression::Parameter(parameter) => arena.intern(ScalarSpec {
            kind: ScalarKind::Parameter(parameter.slot.index.index() as u32),
            logical_type: parameter.return_type(),
            children: Box::new([]),
            local_properties: ScalarLocalProperties::default(),
        }),
        Expression::Function(function) => arena.intern(ScalarSpec {
            kind: ScalarKind::Function {
                routine: function_fingerprint(function),
            },
            logical_type: function.return_type.clone(),
            children: children.into_boxed_slice(),
            local_properties: function_local_properties(function),
        }),
        Expression::Cast(cast) => arena.intern(ScalarSpec {
            kind: ScalarKind::Cast {
                try_cast: cast.try_cast,
            },
            logical_type: cast.target_type.clone(),
            children: children.into_boxed_slice(),
            local_properties: conservative_local_properties(expression),
        }),
        Expression::Conjunction(conjunction) => {
            let kind = match conjunction.conjunction_type {
                ConjunctionType::And => ScalarKind::And,
                ConjunctionType::Or => ScalarKind::Or,
            };
            arena.canonical_conjunction(kind, children)
        }
        Expression::Comparison(comparison) => intern_comparison(
            comparison_op(comparison.comparison_type),
            children[0],
            children[1],
            arena,
        ),
        Expression::Case(case) => arena.intern(ScalarSpec {
            kind: ScalarKind::Case,
            logical_type: case.return_type.clone(),
            children: children.into_boxed_slice(),
            local_properties: conservative_local_properties(expression),
        }),
        Expression::Operator(operator) => arena.intern(ScalarSpec {
            kind: ScalarKind::Operator {
                operator: tagged_fingerprint(30, operator.operator_type as u64),
            },
            logical_type: operator.return_type.clone(),
            children: children.into_boxed_slice(),
            local_properties: conservative_local_properties(expression),
        }),
        Expression::Aggregate(aggregate) => arena.intern(ScalarSpec {
            kind: ScalarKind::Aggregate {
                function: aggregate_fingerprint(aggregate),
            },
            logical_type: aggregate.return_type.clone(),
            children: children.into_boxed_slice(),
            local_properties: conservative_local_properties(expression),
        }),
        Expression::Window(window) => arena.intern(ScalarSpec {
            kind: ScalarKind::Window {
                function: window_fingerprint(window),
            },
            logical_type: window.return_type(),
            children: children.into_boxed_slice(),
            local_properties: conservative_local_properties(expression),
        }),
        Expression::Subquery(_) => Err(paro_error::internal(
            "unplanned scalar subquery reached the Query IR boundary",
        )),
    }
}

fn intern_comparison(
    op: ComparisonOp,
    left: ScalarExprId,
    right: ScalarExprId,
    arena: &mut ScalarArena,
) -> Result<ScalarExprId> {
    let left_type = &arena
        .get(left)
        .ok_or_else(|| paro_error::internal("comparison lost its left scalar"))?
        .logical_type;
    let right_type = &arena
        .get(right)
        .ok_or_else(|| paro_error::internal("comparison lost its right scalar"))?
        .logical_type;
    if left_type == right_type {
        arena.canonical_comparison(op, left, right)
    } else {
        arena.intern(ScalarSpec {
            kind: ScalarKind::Comparison(op),
            logical_type: LogicalType::Boolean,
            children: vec![left, right].into_boxed_slice(),
            local_properties: ScalarLocalProperties {
                may_error: true,
                ..Default::default()
            },
        })
    }
}

fn intern_column_binding(
    binding: ColumnBinding,
    logical_type: LogicalType,
    binding_ids: &mut BindingMap,
    columns: &mut ColumnCatalog,
) -> Result<ColumnId> {
    let type_domain = logical_type_fingerprint(&logical_type);
    let key = (binding.table_index, binding.column_index, type_domain);
    if let Some(column) = binding_ids.get(&key).copied() {
        return Ok(column);
    }
    let column = columns.intern(
        logical_type,
        true,
        ColumnOrigin::Derived {
            key: typed_binding_fingerprint(binding, type_domain),
        },
        ColumnVisibility::Hidden,
        None,
    )?;
    binding_ids.insert(key, column);
    Ok(column)
}

fn function_local_properties(
    function: &paro_planner::expression::FunctionExpression,
) -> ScalarLocalProperties {
    let volatility = match function.function.stability {
        FunctionStability::Consistent => Volatility::Immutable,
        FunctionStability::ConsistentWithinQuery => Volatility::Stable,
        FunctionStability::Volatile => Volatility::Volatile,
    };
    let has_side_effects = function.function.side_effects == FunctionSideEffects::HasSideEffects;
    let depends_on_external_state = function.crosses_execution_boundary();
    ScalarLocalProperties {
        volatility,
        may_error: function.function.error_mode == FunctionErrorMode::CanError,
        has_side_effects,
        depends_on_external_state,
        deterministic: volatility != Volatility::Volatile
            && !has_side_effects
            && !depends_on_external_state,
    }
}

fn conservative_local_properties(expression: &Expression) -> ScalarLocalProperties {
    let evaluation = expression.evaluation_properties();
    ScalarLocalProperties {
        volatility: if evaluation.can_share_evaluation() {
            Volatility::Immutable
        } else {
            Volatility::Volatile
        },
        may_error: !evaluation.is_infallible(),
        has_side_effects: !evaluation.can_share_evaluation(),
        depends_on_external_state: evaluation.is_reorder_fence(),
        deterministic: evaluation.can_share_evaluation() && !evaluation.is_reorder_fence(),
    }
}

fn comparison_op(comparison: ComparisonType) -> ComparisonOp {
    match comparison {
        ComparisonType::Equal => ComparisonOp::Equal,
        ComparisonType::NotEqual => ComparisonOp::NotEqual,
        ComparisonType::LessThan => ComparisonOp::Less,
        ComparisonType::LessThanOrEqual => ComparisonOp::LessOrEqual,
        ComparisonType::GreaterThan => ComparisonOp::Greater,
        ComparisonType::GreaterThanOrEqual => ComparisonOp::GreaterOrEqual,
        ComparisonType::DistinctFrom => ComparisonOp::DistinctFrom,
        ComparisonType::NotDistinctFrom => ComparisonOp::NotDistinctFrom,
    }
}

fn join_comparison(comparison: JoinComparisonType) -> ComparisonOp {
    match comparison {
        JoinComparisonType::Equal => ComparisonOp::Equal,
        JoinComparisonType::NotEqual => ComparisonOp::NotEqual,
        JoinComparisonType::LessThan => ComparisonOp::Less,
        JoinComparisonType::LessThanOrEqual => ComparisonOp::LessOrEqual,
        JoinComparisonType::GreaterThan => ComparisonOp::Greater,
        JoinComparisonType::GreaterThanOrEqual => ComparisonOp::GreaterOrEqual,
        JoinComparisonType::DistinctFrom => ComparisonOp::DistinctFrom,
        JoinComparisonType::NotDistinctFrom => ComparisonOp::NotDistinctFrom,
    }
}

fn function_fingerprint(function: &paro_planner::expression::FunctionExpression) -> Fingerprint {
    let mut builder = StableFingerprintBuilder::default();
    builder.write_u64(40);
    if let Some(identity) = function.routine_identity() {
        encode_routine_identity(&mut builder, identity);
    } else {
        builder.write_bytes(function.function.name.as_bytes());
    }
    encode_signature(
        &mut builder,
        &function.function.arguments,
        &function.return_type,
    );
    builder.finish()
}

fn aggregate_fingerprint(aggregate: &paro_planner::expression::AggregateExpression) -> Fingerprint {
    let mut builder = StableFingerprintBuilder::default();
    builder.write_u64(41);
    builder.write_bytes(aggregate.function.name.as_bytes());
    encode_signature(
        &mut builder,
        &aggregate.function.arguments,
        &aggregate.return_type,
    );
    builder.write_u64(aggregate.aggr_type as u64);
    builder.write_u64(aggregate.filter.is_some() as u64);
    builder.write_u64(aggregate.order_bys.len() as u64);
    for order in &aggregate.order_bys {
        builder.write_u64(order.ascending as u64);
        builder.write_u64(order.nulls_first as u64);
    }
    builder.finish()
}

fn window_fingerprint(window: &paro_planner::expression::WindowExpression) -> Fingerprint {
    let mut builder = StableFingerprintBuilder::default();
    builder.write_u64(42);
    match &window.invocation {
        WindowInvocation::Native { function, .. } => {
            builder.write_u64(0);
            builder.write_bytes(function.name.as_bytes());
            encode_signature(&mut builder, &function.arguments, &function.return_type);
        }
        WindowInvocation::Aggregate(aggregate) => {
            builder.write_u64(1);
            builder.write_fingerprint(aggregate_fingerprint(aggregate));
        }
    }
    builder.write_u64(window.partitions.len() as u64);
    builder.write_u64(window.orders.len() as u64);
    for order in &window.orders {
        builder.write_u64(order.ascending as u64);
        builder.write_u64(order.nulls_first as u64);
    }
    builder.write_u64(window.frame.frame_type as u64);
    builder.write_u64(window.frame.start_is_preceding as u64);
    builder.write_u64(window.frame.end_is_preceding as u64);
    builder.write_u64(window.ignore_nulls as u64);
    builder.finish()
}

fn encode_signature(
    builder: &mut StableFingerprintBuilder,
    arguments: &[LogicalType],
    return_type: &LogicalType,
) {
    builder.write_u64(arguments.len() as u64);
    for argument in arguments {
        super::scalar::encode_logical_type(builder, argument);
    }
    super::scalar::encode_logical_type(builder, return_type);
}

pub(crate) fn encode_routine_identity(
    builder: &mut StableFingerprintBuilder,
    identity: &RoutineCallIdentity,
) {
    match identity {
        RoutineCallIdentity::Catalog {
            routine_id,
            generation,
        } => {
            builder.write_u64(0);
            builder.write_u64(routine_id.raw());
            builder.write_u64(*generation);
        }
        RoutineCallIdentity::Builtin {
            intrinsic,
            semantic_tags,
        } => {
            builder.write_u64(1);
            encode_intrinsic(builder, intrinsic);
            builder.write_u64(semantic_tags.len() as u64);
            for tag in semantic_tags {
                encode_semantic_tag(builder, tag);
            }
        }
    }
}

fn encode_intrinsic(builder: &mut StableFingerprintBuilder, intrinsic: &BuiltinIntrinsicId) {
    let (tag, other) = match intrinsic {
        BuiltinIntrinsicId::Add => (0, None),
        BuiltinIntrinsicId::Subtract => (1, None),
        BuiltinIntrinsicId::Multiply => (2, None),
        BuiltinIntrinsicId::Divide => (3, None),
        BuiltinIntrinsicId::IntegerDivide => (4, None),
        BuiltinIntrinsicId::FullTextMatch => (5, None),
        BuiltinIntrinsicId::FullTextMatchInternal => (6, None),
        BuiltinIntrinsicId::Bm25 => (7, None),
        BuiltinIntrinsicId::Bm25ScoreInternal => (8, None),
        BuiltinIntrinsicId::TsRank => (9, None),
        BuiltinIntrinsicId::TsRankCd => (10, None),
        BuiltinIntrinsicId::ToTsVector => (11, None),
        BuiltinIntrinsicId::PlainToTsQuery => (12, None),
        BuiltinIntrinsicId::ToTsQuery => (13, None),
        BuiltinIntrinsicId::PhraseToTsQuery => (14, None),
        BuiltinIntrinsicId::WebSearchToTsQuery => (15, None),
        BuiltinIntrinsicId::L2Distance => (16, None),
        BuiltinIntrinsicId::L1Distance => (17, None),
        BuiltinIntrinsicId::CosineDistance => (18, None),
        BuiltinIntrinsicId::NegativeInnerProduct => (19, None),
        BuiltinIntrinsicId::SparseDistance => (20, None),
        BuiltinIntrinsicId::Other(value) => (21, Some(value.as_str())),
    };
    builder.write_u64(tag);
    if let Some(other) = other {
        builder.write_bytes(other.as_bytes());
    }
}

fn encode_semantic_tag(builder: &mut StableFingerprintBuilder, tag: &BuiltinSemanticTag) {
    let (tag, other) = match tag {
        BuiltinSemanticTag::Deterministic => (0, None),
        BuiltinSemanticTag::NoSideEffects => (1, None),
        BuiltinSemanticTag::Foldable => (2, None),
        BuiltinSemanticTag::SearchOptimized => (3, None),
        BuiltinSemanticTag::VectorOptimized => (4, None),
        BuiltinSemanticTag::Other(value) => (5, Some(value.as_str())),
    };
    builder.write_u64(tag);
    if let Some(other) = other {
        builder.write_bytes(other.as_bytes());
    }
}

pub(crate) fn value_fingerprint(value: &Value) -> Fingerprint {
    let mut builder = StableFingerprintBuilder::default();
    encode_value(&mut builder, value);
    builder.finish()
}

pub(crate) fn encode_value(builder: &mut StableFingerprintBuilder, value: &Value) {
    macro_rules! integer {
        ($tag:expr, $value:expr) => {{
            builder.write_u64($tag);
            builder.write_bytes(&$value.to_le_bytes());
        }};
    }
    match value {
        Value::Null(ty) => {
            builder.write_u64(0);
            super::scalar::encode_logical_type(builder, ty);
        }
        Value::Boolean(value) => {
            builder.write_u64(1);
            builder.write_u64(*value as u64);
        }
        Value::TinyInt(value) => integer!(2, value),
        Value::SmallInt(value) => integer!(3, value),
        Value::Integer(value) => integer!(4, value),
        Value::BigInt(value) => integer!(5, value),
        Value::HugeInt(value) => integer!(6, value),
        Value::UTinyInt(value) => integer!(7, value),
        Value::USmallInt(value) => integer!(8, value),
        Value::UInteger(value) => integer!(9, value),
        Value::UBigInt(value) => integer!(10, value),
        Value::UHugeInt(value) => integer!(11, value),
        Value::Float(value) => integer!(12, value.to_bits()),
        Value::Double(value) => integer!(13, value.to_bits()),
        Value::Decimal(value, precision, scale) => {
            integer!(14, value);
            builder.write_u64(*precision as u64);
            builder.write_u64(*scale as u64);
        }
        Value::Varchar(value) => {
            builder.write_u64(15);
            builder.write_bytes(value.as_bytes());
        }
        Value::Blob(value) => {
            builder.write_u64(16);
            builder.write_bytes(value);
        }
        Value::Uuid(value) => integer!(17, value),
        Value::Date(value) => integer!(18, value),
        Value::Timestamp(value) => integer!(19, value),
        Value::TimestampTz(value) => integer!(20, value),
        Value::Time(value) => integer!(21, value),
        Value::Interval(months, days, micros) => {
            integer!(22, months);
            builder.write_bytes(&days.to_le_bytes());
            builder.write_bytes(&micros.to_le_bytes());
        }
        Value::List(values, ty) => {
            builder.write_u64(23);
            super::scalar::encode_logical_type(builder, ty);
            builder.write_u64(values.len() as u64);
            for value in values {
                encode_value(builder, value);
            }
        }
        Value::Struct(values, fields) => {
            builder.write_u64(24);
            builder.write_u64(fields.len() as u64);
            for (name, ty) in fields {
                builder.write_bytes(name.as_bytes());
                super::scalar::encode_logical_type(builder, ty);
            }
            for value in values {
                encode_value(builder, value);
            }
        }
        Value::Array(values, ty, size) => {
            builder.write_u64(25);
            super::scalar::encode_logical_type(builder, ty);
            builder.write_u64(*size as u64);
            for value in values {
                encode_value(builder, value);
            }
        }
    }
}

fn tagged_fingerprint(domain: u64, value: u64) -> Fingerprint {
    let mut builder = StableFingerprintBuilder::default();
    builder.write_u64(domain);
    builder.write_u64(value);
    builder.finish()
}

pub(crate) fn logical_type_fingerprint(logical_type: &LogicalType) -> Fingerprint {
    let mut fingerprint = StableFingerprintBuilder::default();
    super::scalar::encode_logical_type(&mut fingerprint, logical_type);
    fingerprint.finish()
}

fn typed_binding_fingerprint(binding: ColumnBinding, type_domain: Fingerprint) -> Fingerprint {
    let mut fingerprint = StableFingerprintBuilder::default();
    fingerprint.write_u64(binding.table_index as u64);
    fingerprint.write_u64(binding.column_index as u64);
    fingerprint.write_fingerprint(type_domain);
    fingerprint.finish()
}
