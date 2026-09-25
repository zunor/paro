// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Executable call descriptors, independent of scalar children.
//!
//! A routine digest is only an interning bucket, not a bound invocation. Keep
//! bind data, modifiers and argument roles here; children live exclusively in
//! the scalar DAG. Native rewrites may replace those children without consulting
//! an extraction tree or reconstructing a kernel from its display name.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_external::routine::bound::BoundRoutineCallMeta;
use paro_external::routine::boundary::PlacementClass;
use paro_function::aggregate::AggregateFunction;
use paro_function::scalar::cast::BoundCastInfo;
use paro_function::scalar::{
    function_data_equals, BoundScalarFunction, FunctionData, FunctionErrorMode,
    FunctionSideEffects, FunctionStability, ScalarDispatch,
};
use paro_function::window::WindowFunction;
use paro_planner::expression::{
    AggregateExpression, AggregateType, Expression, FunctionExpression, OrderByExpression,
    WindowExpression, WindowFrame, WindowFrameBound, WindowFrameType, WindowInvocation,
};

use super::super::ids::Fingerprint;
use paro_planner::physical::scalar_identity::{
    aggregate_binding_fingerprint, function_binding_fingerprint, FrameBoundIdentity,
    WindowBindingIdentity, WindowInvocationIdentity,
};

macro_rules! shared_descriptor {
    ($name:ident, $binding:ident) => {
        #[derive(Debug, Clone)]
        pub struct $name(Arc<$binding>);

        impl $name {
            pub(crate) fn fingerprint(&self) -> Fingerprint {
                self.0.fingerprint
            }
        }

        impl PartialEq for $name {
            fn eq(&self, other: &Self) -> bool {
                Arc::ptr_eq(&self.0, &other.0) || self.0 == other.0
            }
        }
        impl Eq for $name {}
        impl Hash for $name {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.fingerprint().hash(state);
            }
        }
    };
}

#[derive(Debug)]
struct FunctionBinding {
    fingerprint: Fingerprint,
    function: BoundScalarFunction,
    return_type: LogicalType,
    routine_meta: Option<BoundRoutineCallMeta>,
}

impl PartialEq for FunctionBinding {
    fn eq(&self, other: &Self) -> bool {
        self.fingerprint == other.fingerprint
            && self.return_type == other.return_type
            && self.routine_meta == other.routine_meta
            && scalar_kernel_equal(&self.function, &other.function)
    }
}

shared_descriptor!(ScalarFunction, FunctionBinding);

impl ScalarFunction {
    pub fn new(
        function: BoundScalarFunction,
        return_type: LogicalType,
        routine_meta: Option<BoundRoutineCallMeta>,
    ) -> Self {
        Self(Arc::new(FunctionBinding {
            fingerprint: function_binding_fingerprint(
                &function,
                &return_type,
                routine_meta.as_ref().map(|meta| &meta.identity),
            ),
            function,
            return_type,
            routine_meta,
        }))
    }

    pub(crate) fn from_bound(function: &FunctionExpression) -> Self {
        Self::new(
            function.function.clone(),
            function.return_type.clone(),
            function.routine_meta.clone(),
        )
    }

    pub fn function(&self) -> &BoundScalarFunction {
        &self.0.function
    }
    pub fn logical_type(&self) -> &LogicalType {
        &self.0.return_type
    }
    pub fn routine_meta(&self) -> Option<&BoundRoutineCallMeta> {
        self.0.routine_meta.as_ref()
    }

    pub(super) fn local_properties(&self) -> super::ScalarLocalProperties {
        let volatility = match self.function().stability {
            FunctionStability::Consistent => super::Volatility::Immutable,
            FunctionStability::ConsistentWithinQuery => super::Volatility::Stable,
            FunctionStability::Volatile => super::Volatility::Volatile,
        };
        let has_side_effects = self.function().side_effects == FunctionSideEffects::HasSideEffects;
        let depends_on_external_state = self
            .routine_meta()
            .is_some_and(|meta| meta.boundary.placement == PlacementClass::External);
        super::ScalarLocalProperties {
            volatility,
            may_error: self.function().error_mode == FunctionErrorMode::CanError,
            has_side_effects,
            depends_on_external_state,
            deterministic: volatility != super::Volatility::Volatile
                && !has_side_effects
                && !depends_on_external_state,
        }
    }

    pub(super) fn instantiate(&self, children: Vec<Expression>) -> FunctionExpression {
        FunctionExpression {
            function: self.0.function.clone(),
            children,
            return_type: self.0.return_type.clone(),
            routine_meta: self.0.routine_meta.clone(),
        }
    }
}

fn scalar_kernel_equal(left: &BoundScalarFunction, right: &BoundScalarFunction) -> bool {
    if left.shares_binding_with(right) {
        return true;
    }
    macro_rules! optional_fn_equal {
        ($left:expr, $right:expr) => {
            match ($left, $right) {
                (None, None) => true,
                (Some(left), Some(right)) => std::ptr::fn_addr_eq(left, right),
                _ => false,
            }
        };
    }
    let dispatch_equal = match (left.dispatch, right.dispatch) {
        (ScalarDispatch::Direct(left), ScalarDispatch::Direct(right))
        | (ScalarDispatch::Variadic(left), ScalarDispatch::Variadic(right)) => {
            std::ptr::fn_addr_eq(left, right)
        }
        _ => false,
    };
    left.name == right.name
        && left.arguments == right.arguments
        && left.return_type == right.return_type
        && left.varargs == right.varargs
        && left.stability == right.stability
        && left.null_handling == right.null_handling
        && left.side_effects == right.side_effects
        && left.error_mode == right.error_mode
        && left.dictionary_strategy == right.dictionary_strategy
        && left.predicate_projection == right.predicate_projection
        && dispatch_equal
        && optional_fn_equal!(left.init_local_state, right.init_local_state)
        && optional_fn_equal!(left.statistics, right.statistics)
        && function_data_equals(left.bind_data.as_ref(), right.bind_data.as_ref())
}

/// The actual registry binding, not a request to rebind by SQL type later.
#[derive(Debug, Clone)]
pub struct ScalarCast(Arc<BoundCastInfo>);

impl ScalarCast {
    pub fn new(info: BoundCastInfo) -> Self {
        Self(Arc::new(info))
    }
    pub fn binding(&self) -> &BoundCastInfo {
        &self.0
    }
}
impl PartialEq for ScalarCast {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0) || self.0.execution_semantics_equal(&other.0)
    }
}
impl Eq for ScalarCast {}
impl Hash for ScalarCast {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Source/target types own the stable scalar bucket. Opaque executable
        // metadata is checked exactly, never by address hashing.
        0_u8.hash(state);
    }
}

/// A sort operand's position is in the scalar child list, not an Expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScalarSort {
    pub ascending: bool,
    pub nulls_first: bool,
}

impl ScalarSort {
    fn from_bound(order: &OrderByExpression) -> Self {
        Self {
            ascending: order.ascending,
            nulls_first: order.nulls_first,
        }
    }
    fn instantiate(self, expression: Expression) -> OrderByExpression {
        OrderByExpression {
            expression,
            ascending: self.ascending,
            nulls_first: self.nulls_first,
        }
    }
}

#[derive(Debug)]
struct AggregateBinding {
    fingerprint: Fingerprint,
    function: AggregateFunction,
    return_type: LogicalType,
    argument_count: usize,
    distinct: AggregateType,
    has_filter: bool,
    orders: Box<[ScalarSort]>,
    bind_info: Option<Arc<dyn FunctionData>>,
}

/// Child roles for a new aggregate produced by a native rule. No executable
/// expression placeholders are needed to bind a different aggregate kernel.
#[derive(Debug, Clone)]
pub struct ScalarAggregateSpec {
    pub function: AggregateFunction,
    pub return_type: LogicalType,
    pub argument_count: usize,
    pub distinct: AggregateType,
    pub has_filter: bool,
    pub orders: Box<[ScalarSort]>,
    pub bind_info: Option<Arc<dyn FunctionData>>,
}

impl PartialEq for AggregateBinding {
    fn eq(&self, other: &Self) -> bool {
        self.fingerprint == other.fingerprint
            && self.return_type == other.return_type
            && self.argument_count == other.argument_count
            && self.distinct == other.distinct
            && self.has_filter == other.has_filter
            && self.orders == other.orders
            && self.function.name == other.function.name
            && self.function.execution_semantics_equal(&other.function)
            // The singleton law is optimizer evidence, not execution code;
            // kernel equality deliberately excludes it, Memo identity cannot.
            && self.function.singleton_merge() == other.function.singleton_merge()
            && function_data_equals(self.bind_info.as_ref(), other.bind_info.as_ref())
    }
}

shared_descriptor!(ScalarAggregate, AggregateBinding);

impl ScalarAggregate {
    pub fn new(spec: ScalarAggregateSpec) -> Self {
        Self(Arc::new(AggregateBinding {
            fingerprint: aggregate_binding_fingerprint(
                &spec.function,
                &spec.return_type,
                spec.distinct,
                spec.has_filter,
                spec.orders
                    .iter()
                    .map(|order| (order.ascending, order.nulls_first)),
                spec.bind_info.as_ref(),
            ),
            function: spec.function,
            return_type: spec.return_type,
            argument_count: spec.argument_count,
            distinct: spec.distinct,
            has_filter: spec.has_filter,
            orders: spec.orders,
            bind_info: spec.bind_info,
        }))
    }

    pub(crate) fn from_bound(aggregate: &AggregateExpression) -> Self {
        Self::new(ScalarAggregateSpec {
            function: aggregate.function.clone(),
            return_type: aggregate.return_type.clone(),
            argument_count: aggregate.children.len(),
            distinct: aggregate.aggr_type,
            has_filter: aggregate.filter.is_some(),
            orders: aggregate
                .order_bys
                .iter()
                .map(ScalarSort::from_bound)
                .collect(),
            bind_info: aggregate.bind_info.clone(),
        })
    }
    pub fn function(&self) -> &AggregateFunction {
        &self.0.function
    }
    pub fn logical_type(&self) -> &LogicalType {
        &self.0.return_type
    }
    pub fn argument_count(&self) -> usize {
        self.0.argument_count
    }
    pub fn is_distinct(&self) -> bool {
        self.0.distinct == AggregateType::Distinct
    }
    pub fn filter_ordinal(&self) -> Option<usize> {
        self.0.has_filter.then_some(self.0.argument_count)
    }
    pub fn orders(&self) -> &[ScalarSort] {
        &self.0.orders
    }
    pub fn child_count(&self) -> usize {
        self.0.argument_count + usize::from(self.0.has_filter) + self.0.orders.len()
    }

    fn instantiate_from(
        &self,
        children: &mut impl Iterator<Item = Expression>,
    ) -> Result<AggregateExpression> {
        let arguments = (0..self.argument_count())
            .map(|_| next_child(children))
            .collect::<Result<Vec<_>>>()?;
        let filter = self
            .0
            .has_filter
            .then(|| next_child(children).map(Box::new))
            .transpose()?;
        let order_bys = self
            .0
            .orders
            .iter()
            .map(|order| next_child(children).map(|expression| order.instantiate(expression)))
            .collect::<Result<Vec<_>>>()?;
        Ok(AggregateExpression {
            function: self.0.function.clone(),
            children: arguments,
            return_type: self.0.return_type.clone(),
            aggr_type: self.0.distinct,
            filter,
            order_bys,
            bind_info: self.0.bind_info.clone(),
        })
    }

    pub(super) fn instantiate(&self, children: Vec<Expression>) -> Result<AggregateExpression> {
        if children.len() != self.child_count() {
            return Err(paro_error::internal(
                "native aggregate child roles disagree with its invocation",
            ));
        }
        self.instantiate_from(&mut children.into_iter())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarFrameBound {
    Unbounded,
    CurrentRow,
    Offset,
}

impl ScalarFrameBound {
    fn from_bound(bound: &WindowFrameBound) -> Self {
        match bound {
            WindowFrameBound::Unbounded => Self::Unbounded,
            WindowFrameBound::CurrentRow => Self::CurrentRow,
            WindowFrameBound::Offset(_) => Self::Offset,
        }
    }
    fn instantiate(
        self,
        children: &mut impl Iterator<Item = Expression>,
    ) -> Result<WindowFrameBound> {
        Ok(match self {
            Self::Unbounded => WindowFrameBound::Unbounded,
            Self::CurrentRow => WindowFrameBound::CurrentRow,
            Self::Offset => WindowFrameBound::Offset(Box::new(next_child(children)?)),
        })
    }
}

#[derive(Debug, Clone)]
pub enum ScalarWindowInvocation {
    Native {
        function: WindowFunction,
        argument_count: usize,
    },
    Aggregate(ScalarAggregate),
}

impl PartialEq for ScalarWindowInvocation {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Aggregate(left), Self::Aggregate(right)) => left == right,
            (
                Self::Native {
                    function: left,
                    argument_count: lc,
                },
                Self::Native {
                    function: right,
                    argument_count: rc,
                },
            ) => {
                lc == rc
                    && left.name == right.name
                    && left.arguments == right.arguments
                    && left.return_type == right.return_type
                    && left.function_type == right.function_type
            }
            _ => false,
        }
    }
}

#[derive(Debug, PartialEq)]
struct WindowBinding {
    fingerprint: Fingerprint,
    spec: ScalarWindowSpec,
}

/// Complete window invocation and child roles, without expression ownership.
#[derive(Debug, Clone, PartialEq)]
pub struct ScalarWindowSpec {
    pub invocation: ScalarWindowInvocation,
    pub partition_count: usize,
    pub orders: Box<[ScalarSort]>,
    pub frame_type: WindowFrameType,
    pub start: ScalarFrameBound,
    pub start_is_preceding: bool,
    pub end: ScalarFrameBound,
    pub end_is_preceding: bool,
    pub ignore_nulls: bool,
}

impl std::ops::Deref for WindowBinding {
    type Target = ScalarWindowSpec;
    fn deref(&self) -> &Self::Target {
        &self.spec
    }
}

shared_descriptor!(ScalarWindow, WindowBinding);

impl ScalarWindow {
    pub fn new(spec: ScalarWindowSpec) -> Self {
        let bound = |bound| match bound {
            ScalarFrameBound::Unbounded => FrameBoundIdentity::Unbounded,
            ScalarFrameBound::CurrentRow => FrameBoundIdentity::CurrentRow,
            ScalarFrameBound::Offset => FrameBoundIdentity::Offset,
        };
        let fingerprint = WindowBindingIdentity {
            invocation: match &spec.invocation {
                ScalarWindowInvocation::Native { function, .. } => {
                    WindowInvocationIdentity::Native(function)
                }
                ScalarWindowInvocation::Aggregate(aggregate) => {
                    WindowInvocationIdentity::Aggregate(aggregate.fingerprint())
                }
            },
            partition_count: spec.partition_count,
            orders: spec.orders.iter().map(|o| (o.ascending, o.nulls_first)),
            frame_type: spec.frame_type,
            start: bound(spec.start),
            end: bound(spec.end),
            start_is_preceding: spec.start_is_preceding,
            end_is_preceding: spec.end_is_preceding,
            ignore_nulls: spec.ignore_nulls,
        }
        .fingerprint();
        Self(Arc::new(WindowBinding { fingerprint, spec }))
    }

    pub(crate) fn from_bound(window: &WindowExpression) -> Self {
        Self::new(ScalarWindowSpec {
            invocation: match &window.invocation {
                WindowInvocation::Native {
                    function,
                    arguments,
                } => ScalarWindowInvocation::Native {
                    function: function.clone(),
                    argument_count: arguments.len(),
                },
                WindowInvocation::Aggregate(aggregate) => {
                    ScalarWindowInvocation::Aggregate(ScalarAggregate::from_bound(aggregate))
                }
            },
            partition_count: window.partitions.len(),
            orders: window.orders.iter().map(ScalarSort::from_bound).collect(),
            frame_type: window.frame.frame_type,
            start: ScalarFrameBound::from_bound(&window.frame.start_bound),
            start_is_preceding: window.frame.start_is_preceding,
            end: ScalarFrameBound::from_bound(&window.frame.end_bound),
            end_is_preceding: window.frame.end_is_preceding,
            ignore_nulls: window.ignore_nulls,
        })
    }
    pub fn invocation(&self) -> &ScalarWindowInvocation {
        &self.0.invocation
    }
    pub fn logical_type(&self) -> &LogicalType {
        match &self.0.invocation {
            ScalarWindowInvocation::Native { function, .. } => &function.return_type,
            ScalarWindowInvocation::Aggregate(aggregate) => aggregate.logical_type(),
        }
    }
    pub fn child_count(&self) -> usize {
        let arguments = match &self.0.invocation {
            ScalarWindowInvocation::Native { argument_count, .. } => *argument_count,
            ScalarWindowInvocation::Aggregate(aggregate) => aggregate.child_count(),
        };
        arguments
            + self.0.partition_count
            + self.0.orders.len()
            + usize::from(self.0.start == ScalarFrameBound::Offset)
            + usize::from(self.0.end == ScalarFrameBound::Offset)
    }
    pub(super) fn instantiate(&self, children: Vec<Expression>) -> Result<WindowExpression> {
        if children.len() != self.child_count() {
            return Err(paro_error::internal(
                "native window child roles disagree with its invocation",
            ));
        }
        let mut children = children.into_iter();
        let invocation = match &self.0.invocation {
            ScalarWindowInvocation::Native {
                function,
                argument_count,
            } => WindowInvocation::Native {
                function: function.clone(),
                arguments: (0..*argument_count)
                    .map(|_| next_child(&mut children))
                    .collect::<Result<_>>()?,
            },
            ScalarWindowInvocation::Aggregate(aggregate) => {
                WindowInvocation::Aggregate(aggregate.instantiate_from(&mut children)?)
            }
        };
        let partitions = (0..self.0.partition_count)
            .map(|_| next_child(&mut children))
            .collect::<Result<_>>()?;
        let orders = self
            .0
            .orders
            .iter()
            .map(|order| next_child(&mut children).map(|expression| order.instantiate(expression)))
            .collect::<Result<_>>()?;
        let frame = WindowFrame {
            frame_type: self.0.frame_type,
            start_bound: self.0.start.instantiate(&mut children)?,
            start_is_preceding: self.0.start_is_preceding,
            end_bound: self.0.end.instantiate(&mut children)?,
            end_is_preceding: self.0.end_is_preceding,
        };
        Ok(WindowExpression {
            invocation,
            partitions,
            orders,
            frame,
            ignore_nulls: self.0.ignore_nulls,
        })
    }
}

fn next_child(children: &mut impl Iterator<Item = Expression>) -> Result<Expression> {
    children
        .next()
        .ok_or_else(|| paro_error::internal("native call lost a scalar operand"))
}
