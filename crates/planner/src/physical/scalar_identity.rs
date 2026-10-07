// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Canonical bound-expression and value encoders, independent of optimizer IR.

use super::identity::{Fingerprint, StableFingerprintBuilder};
use crate::expression::{ConjunctionType, Expression, ExpressionIdentity, ExpressionIterator};
use paro_common::runtime_value::Value;
use paro_common::types::LogicalType;
use paro_external::routine::identity::{
    BuiltinIntrinsicId, BuiltinSemanticTag, RoutineCallIdentity,
};
use std::collections::{HashMap, HashSet};

/// Bound invocation identity excludes scalar operands; their role-ordered
/// identities are encoded by the enclosing expression or scalar DAG.
pub enum WindowInvocationIdentity<'a> {
    Native(&'a paro_function::window::WindowFunction),
    Aggregate(Fingerprint),
}

#[derive(Clone, Copy)]
pub enum FrameBoundIdentity {
    Unbounded,
    CurrentRow,
    Offset,
}

impl From<&crate::expression::WindowFrameBound> for FrameBoundIdentity {
    fn from(bound: &crate::expression::WindowFrameBound) -> Self {
        use crate::expression::WindowFrameBound;
        match bound {
            WindowFrameBound::Unbounded => Self::Unbounded,
            WindowFrameBound::CurrentRow => Self::CurrentRow,
            WindowFrameBound::Offset(_) => Self::Offset,
        }
    }
}

pub struct WindowBindingIdentity<'a, I> {
    pub invocation: WindowInvocationIdentity<'a>,
    pub partition_count: usize,
    pub orders: I,
    pub frame_type: crate::expression::WindowFrameType,
    pub start: FrameBoundIdentity,
    pub end: FrameBoundIdentity,
    pub start_is_preceding: bool,
    pub end_is_preceding: bool,
    pub ignore_nulls: bool,
}

impl<I: ExactSizeIterator<Item = (bool, bool)>> WindowBindingIdentity<'_, I> {
    pub fn fingerprint(self) -> Fingerprint {
        let mut builder = StableFingerprintBuilder::default();
        builder.write_u64(42);
        match self.invocation {
            WindowInvocationIdentity::Native(function) => {
                builder.write_u64(0);
                builder.write_bytes(function.name.as_bytes());
                encode_signature(&mut builder, &function.arguments, &function.return_type);
            }
            WindowInvocationIdentity::Aggregate(fingerprint) => {
                builder.write_u64(1);
                builder.write_fingerprint(fingerprint);
            }
        }
        builder.write_u64(self.partition_count as u64);
        builder.write_u64(self.orders.len() as u64);
        for (ascending, nulls_first) in self.orders {
            builder.write_u64(ascending as u64);
            builder.write_u64(nulls_first as u64);
        }
        builder.write_u64(self.frame_type as u64);
        builder.write_u64(self.start_is_preceding as u64);
        builder.write_u64(self.end_is_preceding as u64);
        builder.write_u64(self.ignore_nulls as u64);
        builder.write_u64(self.start as u64);
        builder.write_u64(self.end as u64);
        builder.finish()
    }
}

pub fn function_fingerprint(function: &crate::expression::FunctionExpression) -> Fingerprint {
    function_binding_fingerprint(
        &function.function,
        &function.return_type,
        function.routine_identity(),
    )
}

pub fn function_binding_fingerprint(
    function: &paro_function::scalar::BoundScalarFunction,
    return_type: &LogicalType,
    identity: Option<&RoutineCallIdentity>,
) -> Fingerprint {
    let mut builder = StableFingerprintBuilder::default();
    builder.write_u64(40);
    if let Some(identity) = identity {
        encode_routine_identity(&mut builder, identity);
    } else {
        builder.write_bytes(function.name.as_bytes());
    }
    encode_signature(&mut builder, &function.arguments, return_type);
    if let Some(data) = &function.bind_data {
        builder.write_u64(1);
        builder.write_u64(data.fingerprint());
    }
    builder.finish()
}

pub fn aggregate_fingerprint(aggregate: &crate::expression::AggregateExpression) -> Fingerprint {
    aggregate_binding_fingerprint(
        &aggregate.function,
        &aggregate.return_type,
        aggregate.aggr_type,
        aggregate.filter.is_some(),
        aggregate
            .order_bys
            .iter()
            .map(|order| (order.ascending, order.nulls_first)),
        aggregate.bind_info.as_ref(),
    )
}

pub fn aggregate_binding_fingerprint(
    function: &paro_function::aggregate::AggregateFunction,
    return_type: &LogicalType,
    distinct: crate::expression::AggregateType,
    has_filter: bool,
    orders: impl ExactSizeIterator<Item = (bool, bool)>,
    bind_info: Option<&std::sync::Arc<dyn paro_function::scalar::FunctionData>>,
) -> Fingerprint {
    let mut builder = StableFingerprintBuilder::default();
    builder.write_u64(41);
    builder.write_bytes(function.name.as_bytes());
    encode_signature(&mut builder, &function.arguments, return_type);
    builder.write_u64(distinct as u64);
    builder.write_u64(has_filter as u64);
    builder.write_u64(orders.len() as u64);
    for (ascending, nulls_first) in orders {
        builder.write_u64(ascending as u64);
        builder.write_u64(nulls_first as u64);
    }
    for (role, data) in [(0, function.bind_data.as_ref()), (1, bind_info)] {
        if let Some(data) = data {
            builder.write_u64(role);
            builder.write_u64(data.fingerprint());
        }
    }
    builder.finish()
}

pub fn window_fingerprint(window: &crate::expression::WindowExpression) -> Fingerprint {
    use crate::expression::WindowInvocation;
    WindowBindingIdentity {
        invocation: match &window.invocation {
            WindowInvocation::Native { function, .. } => WindowInvocationIdentity::Native(function),
            WindowInvocation::Aggregate(aggregate) => {
                WindowInvocationIdentity::Aggregate(aggregate_fingerprint(aggregate))
            }
        },
        partition_count: window.partitions.len(),
        orders: window.orders.iter().map(|o| (o.ascending, o.nulls_first)),
        frame_type: window.frame.frame_type,
        start: (&window.frame.start_bound).into(),
        end: (&window.frame.end_bound).into(),
        start_is_preceding: window.frame.start_is_preceding,
        end_is_preceding: window.frame.end_is_preceding,
        ignore_nulls: window.ignore_nulls,
    }
    .fingerprint()
}

pub fn encode_signature(
    builder: &mut StableFingerprintBuilder,
    arguments: &[LogicalType],
    return_type: &LogicalType,
) {
    builder.write_u64(arguments.len() as u64);
    for argument in arguments {
        encode_logical_type(builder, argument);
    }
    encode_logical_type(builder, return_type);
}

pub fn encode_routine_identity(
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

pub fn value_fingerprint(value: &Value) -> Fingerprint {
    let mut builder = StableFingerprintBuilder::default();
    encode_value(&mut builder, value);
    builder.finish()
}

/// Stable structural identity for a bound expression used by planner-side
/// domain proofs.  This deliberately does not depend on arena-local
/// `ColumnId`s, insertion order, or pointer addresses.  Conjunction children
/// are normalized as a multiset because CTE domain equality treats nested
/// AND/OR arms as commutative and idempotent.
pub fn expression_fingerprint(expression: &Expression) -> Fingerprint {
    // Expression trees can be generated from very long IN/OR lists.  Keep
    // this identity path explicitly post-order just like logical-plan
    // staging; a query must not be able to exhaust the native stack merely by
    // asking the CTE domain interner for a fingerprint.
    let mut pending = vec![(expression, false)];
    let mut fingerprints = HashMap::<ExpressionIdentity, Fingerprint>::new();
    while let Some((current, visited)) = pending.pop() {
        if fingerprints.contains_key(&current.allocation_identity()) {
            continue;
        }
        if visited {
            let mut builder = StableFingerprintBuilder::default();
            encode_expression_node_fingerprint(&mut builder, current, &fingerprints);
            fingerprints.insert(current.allocation_identity(), builder.finish());
            continue;
        }
        pending.push((current, true));
        let mut children = Vec::new();
        if let Expression::Conjunction(conjunction) = current {
            // Fingerprint only maximal associative runs. Fingerprinting each
            // nested prefix and then flattening it again is quadratic on a
            // generated OR chain, even with an explicit traversal stack.
            let mut run = conjunction.children.iter().collect::<Vec<_>>();
            let mut expanded = HashSet::new();
            while let Some(child) = run.pop() {
                if !expanded.insert(child.allocation_identity()) {
                    continue;
                }
                if let Expression::Conjunction(nested) = child {
                    if nested.conjunction_type == conjunction.conjunction_type {
                        run.extend(nested.children.iter());
                        continue;
                    }
                }
                children.push(child);
            }
        } else {
            ExpressionIterator::enumerate_children(current, |child| children.push(child));
        }
        pending.extend(children.into_iter().rev().map(|child| (child, false)));
    }
    fingerprints[&expression.allocation_identity()]
}

/// Physical evaluation identity preserves child order and multiplicity. Domain
/// proofs intentionally flatten AND/OR as sets; using that normalization for
/// execution would erase short-circuit order and repeated volatile operands.
pub fn physical_expression_fingerprint(expression: &Expression) -> Fingerprint {
    let mut pending = vec![(expression, false)];
    let mut fingerprints = HashMap::<ExpressionIdentity, Fingerprint>::new();
    while let Some((current, visited)) = pending.pop() {
        if fingerprints.contains_key(&current.allocation_identity()) {
            continue;
        }
        if !visited {
            pending.push((current, true));
            ExpressionIterator::enumerate_children(current, |child| pending.push((child, false)));
            continue;
        }
        let mut builder = StableFingerprintBuilder::default();
        builder.write_bytes(b"paro.physical-expression.v1");
        encode_logical_type(&mut builder, &current.return_type());
        if let Expression::Conjunction(conjunction) = current {
            builder.write_u64(match conjunction.conjunction_type {
                ConjunctionType::And => 4,
                ConjunctionType::Or => 5,
            });
            builder.write_u64(conjunction.children.len() as u64);
            for child in &conjunction.children {
                builder.write_fingerprint(expression_child_fingerprint(&fingerprints, child));
            }
        } else {
            encode_expression_node_fingerprint(&mut builder, current, &fingerprints);
        }
        fingerprints.insert(current.allocation_identity(), builder.finish());
    }
    fingerprints[&expression.allocation_identity()]
}

fn expression_child_fingerprint(
    fingerprints: &HashMap<ExpressionIdentity, Fingerprint>,
    expression: &Expression,
) -> Fingerprint {
    *fingerprints
        .get(&expression.allocation_identity())
        .expect("expression post-order must fingerprint every child")
}

fn encode_expression_node_fingerprint(
    builder: &mut StableFingerprintBuilder,
    expression: &Expression,
    fingerprints: &HashMap<ExpressionIdentity, Fingerprint>,
) {
    let child_fp = |expression: &Expression| expression_child_fingerprint(fingerprints, expression);
    match expression {
        Expression::Constant(constant) => {
            builder.write_u64(0);
            builder.write_fingerprint(value_fingerprint(&constant.value));
        }
        Expression::ColumnRef(column) => {
            builder.write_u64(1);
            builder.write_u64(column.binding.table_index as u64);
            builder.write_u64(column.binding.column_index as u64);
            builder.write_u64(column.depth as u64);
        }
        Expression::Function(function) => {
            builder.write_u64(2);
            builder.write_fingerprint(function_fingerprint(function));
            builder.write_u64(function.children.len() as u64);
            for child in &function.children {
                builder.write_fingerprint(child_fp(child));
            }
        }
        Expression::Cast(cast) => {
            builder.write_u64(3);
            encode_logical_type(builder, &cast.target_type);
            builder.write_u64(cast.try_cast as u64);
            builder.write_fingerprint(child_fp(&cast.child));
        }
        Expression::Conjunction(conjunction) => {
            builder.write_u64(match conjunction.conjunction_type {
                ConjunctionType::And => 4,
                ConjunctionType::Or => 5,
            });
            // Domain proofs flatten nested conjunctions of the same kind and
            // compare their arms as an idempotent set.  Encode that exact
            // normalization here so the fingerprint remains a necessary
            // condition for the structural proof (never a source of false
            // negatives).
            let mut leaves = Vec::new();
            let mut pending = conjunction.children.iter().collect::<Vec<_>>();
            let mut expanded = HashSet::new();
            while let Some(child) = pending.pop() {
                if !expanded.insert(child.allocation_identity()) {
                    continue;
                }
                if let Expression::Conjunction(nested) = child {
                    if nested.conjunction_type == conjunction.conjunction_type {
                        pending.extend(nested.children.iter());
                        continue;
                    }
                }
                leaves.push(child);
            }
            let mut children = leaves.into_iter().map(child_fp).collect::<Vec<_>>();
            children.sort_unstable();
            children.dedup();
            builder.write_u64(children.len() as u64);
            for fingerprint in children {
                builder.write_fingerprint(fingerprint);
            }
        }
        Expression::Case(case) => {
            builder.write_u64(6);
            builder.write_fingerprint(child_fp(&case.check));
            builder.write_fingerprint(child_fp(&case.result_if_true));
            builder.write_fingerprint(child_fp(&case.result_if_false));
        }
        Expression::Comparison(comparison) => {
            builder.write_u64(7);
            builder.write_u64(comparison.comparison_type as u64);
            builder.write_fingerprint(child_fp(&comparison.left));
            builder.write_fingerprint(child_fp(&comparison.right));
        }
        Expression::Operator(operator) => {
            builder.write_u64(8);
            builder.write_u64(operator.operator_type as u64);
            builder.write_u64(operator.children.len() as u64);
            for child in &operator.children {
                builder.write_fingerprint(child_fp(child));
            }
        }
        Expression::Parameter(parameter) => {
            builder.write_u64(9);
            builder.write_u64(parameter.slot.index.index() as u64);
            encode_logical_type(builder, &parameter.slot.ty);
        }
        Expression::Reference(reference) => {
            builder.write_u64(10);
            builder.write_u64(reference.index as u64);
        }
        Expression::Aggregate(aggregate) => {
            builder.write_u64(11);
            builder.write_fingerprint(aggregate_fingerprint(aggregate));
            for child in &aggregate.children {
                builder.write_fingerprint(child_fp(child));
            }
            if let Some(filter) = &aggregate.filter {
                builder.write_u64(1);
                builder.write_fingerprint(child_fp(filter));
            } else {
                builder.write_u64(0);
            }
            for order in &aggregate.order_bys {
                builder.write_u64(order.ascending as u64);
                builder.write_u64(order.nulls_first as u64);
                builder.write_fingerprint(child_fp(&order.expression));
            }
        }
        Expression::Subquery(subquery) => {
            // Unplanned scalar subqueries are rejected at the Query IR
            // boundary. Keep a typed marker here so a malformed proof cannot
            // alias a scalar expression of another variant.
            builder.write_u64(12);
            builder.write_bytes(format!("{subquery:?}").as_bytes());
        }
        Expression::Window(window) => {
            builder.write_u64(13);
            builder.write_fingerprint(window_fingerprint(window));
            for partition in &window.partitions {
                builder.write_fingerprint(child_fp(partition));
            }
            for order in &window.orders {
                builder.write_u64(order.ascending as u64);
                builder.write_u64(order.nulls_first as u64);
                builder.write_fingerprint(child_fp(&order.expression));
            }
            encode_frame_bound_fingerprint(builder, &window.frame.start_bound, fingerprints);
            encode_frame_bound_fingerprint(builder, &window.frame.end_bound, fingerprints);
        }
    }
}

fn encode_frame_bound_fingerprint(
    builder: &mut StableFingerprintBuilder,
    bound: &crate::expression::WindowFrameBound,
    fingerprints: &HashMap<ExpressionIdentity, Fingerprint>,
) {
    match bound {
        crate::expression::WindowFrameBound::Unbounded => builder.write_u64(0),
        crate::expression::WindowFrameBound::CurrentRow => builder.write_u64(1),
        crate::expression::WindowFrameBound::Offset(expression) => {
            builder.write_u64(2);
            builder.write_fingerprint(expression_child_fingerprint(fingerprints, expression));
        }
    }
}

pub fn encode_value(builder: &mut StableFingerprintBuilder, value: &Value) {
    macro_rules! integer {
        ($tag:expr, $value:expr) => {{
            builder.write_u64($tag);
            builder.write_bytes(&$value.to_le_bytes());
        }};
    }
    // Nested values are user-visible constants and can be arbitrarily deep.
    // Encode them with an explicit work stack, preserving the exact order of
    // the previous recursive representation while making fingerprinting safe
    // for generated LIST/STRUCT/ARRAY literals.
    let mut pending = vec![value];
    while let Some(value) = pending.pop() {
        match value {
            Value::Null(ty) => {
                builder.write_u64(0);
                encode_logical_type(builder, ty);
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
                encode_logical_type(builder, ty);
                builder.write_u64(values.len() as u64);
                pending.extend(values.iter().rev());
            }
            Value::Struct(values, fields) => {
                builder.write_u64(24);
                builder.write_u64(fields.len() as u64);
                for (name, ty) in fields {
                    builder.write_bytes(name.as_bytes());
                    encode_logical_type(builder, ty);
                }
                pending.extend(values.iter().rev());
            }
            Value::Array(values, ty, size) => {
                builder.write_u64(25);
                encode_logical_type(builder, ty);
                builder.write_u64(*size as u64);
                pending.extend(values.iter().rev());
            }
        }
    }
}

pub fn logical_type_fingerprint(logical_type: &LogicalType) -> Fingerprint {
    let mut fingerprint = StableFingerprintBuilder::default();
    encode_logical_type(&mut fingerprint, logical_type);
    fingerprint.finish()
}

pub fn encode_logical_type(builder: &mut StableFingerprintBuilder, ty: &LogicalType) {
    // Logical types can be nested independently of the expression tree (for
    // example, a deeply nested LIST/STRUCT literal).  Keep this identity
    // encoder stack-safe as well; expression_fingerprint and value_fingerprint
    // both call it on their hot cache-key paths.
    enum Task<'a> {
        Type(&'a LogicalType),
        StructName(&'a str),
        U64(u64),
    }

    let mut pending = vec![Task::Type(ty)];
    while let Some(task) = pending.pop() {
        match task {
            Task::Type(ty) => {
                builder.write_u64(ty.type_id() as u64);
                match ty {
                    LogicalType::Decimal { precision, scale } => {
                        builder.write_u64(*precision as u64);
                        builder.write_u64(*scale as u64);
                    }
                    LogicalType::VarcharCollation(collation) => {
                        builder.write_bytes(collation.as_bytes())
                    }
                    LogicalType::IntegerLiteral(value) => builder.write_u64(*value as u64),
                    LogicalType::Array(child, length) => {
                        // The child encoding precedes the fixed length.
                        pending.push(Task::U64(*length as u64));
                        pending.push(Task::Type(child));
                    }
                    LogicalType::List(child) => pending.push(Task::Type(child)),
                    LogicalType::Struct(fields) => {
                        builder.write_u64(fields.len() as u64);
                        // Push in reverse so each field is encoded as
                        // name, type, matching the historical wire format.
                        for (name, field) in fields.iter().rev() {
                            pending.push(Task::Type(field));
                            pending.push(Task::StructName(name));
                        }
                    }
                    _ => {}
                }
            }
            Task::StructName(name) => builder.write_bytes(name.as_bytes()),
            Task::U64(value) => builder.write_u64(value),
        }
    }
}
