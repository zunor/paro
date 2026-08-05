// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Vectorized Cast Framework
//!
//! This module implements the core infrastructure for vectorized type casting,
//!
//!
//!
//! ## Dependencies Check
//! - Vector: ✅
//! - Result: ✅
//! - Allocator: 🔵 Framework (via Vector)

use paro_common::error::{self as paro_error, Result};
use paro_common::types::{LogicalType, PhysicalType};
use paro_common::vector::Vector;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

pub mod array_casts;
pub mod boolean_casts;
pub mod date_casts;
pub mod decimal_casts;
pub mod numeric_casts;
pub mod string_casts;
pub mod struct_casts;

use crate::scalar::FunctionExecContext;

/// Input passed to cast binding functions.
pub struct BindCastInput<'a> {
    pub cast_functions: &'a CastFunctionSet,
    // TODO: Add ClientContext if needed
}

impl<'a> BindCastInput<'a> {
    pub fn new(cast_functions: &'a CastFunctionSet) -> Self {
        Self { cast_functions }
    }

    pub fn get_cast_function(
        &self,
        source: &LogicalType,
        target: &LogicalType,
    ) -> Result<BoundCastInfo> {
        self.cast_functions.get_cast_function(source, target)
    }
}
pub type BindCastFunctionFn = fn(
    input: &BindCastInput,
    source: &LogicalType,
    target: &LogicalType,
) -> Result<Option<BoundCastInfo>>;

/// A registration for a dynamic cast binding function.
pub struct BindCastFunction {
    pub function: BindCastFunctionFn,
}

/// Runtime context passed to a cast dispatch during execution.
pub struct CastExecCtx<'a> {
    /// Runtime execution contract for the current batch.
    pub runtime: &'a dyn FunctionExecContext,
    /// Whether to return NULL on failure (TRY_CAST).
    pub try_cast: bool,
    /// Optional data bound during the binding phase.
    pub cast_data: Option<&'a dyn BoundCastData>,
}

pub type FixedCastFn =
    fn(source: &Vector, result: &mut Vector, count: usize, ctx: &CastExecCtx<'_>) -> Result<bool>;
pub type VarlenCastFn =
    fn(source: &Vector, result: &mut Vector, count: usize, ctx: &CastExecCtx<'_>) -> Result<bool>;
pub type ArrayCastFn =
    fn(source: &Vector, result: &mut Vector, count: usize, ctx: &CastExecCtx<'_>) -> Result<bool>;
pub type StructCastFn =
    fn(source: &Vector, result: &mut Vector, count: usize, ctx: &CastExecCtx<'_>) -> Result<bool>;

#[derive(Clone, Copy)]
pub enum CastDispatch {
    Fixed(FixedCastFn),
    Varlen(VarlenCastFn),
    Array(ArrayCastFn),
    Struct(StructCastFn),
}

/// Describes whether a cast can be evaluated without the query runtime context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CastContextDependency {
    Independent,
    Runtime,
}

impl CastContextDependency {
    pub fn combine(self, other: Self) -> Self {
        if matches!(self, Self::Runtime) || matches!(other, Self::Runtime) {
            Self::Runtime
        } else {
            Self::Independent
        }
    }
}

/// Metadata stored in a bound cast expression.
pub trait BoundCastData: fmt::Debug + Send + Sync {
    fn copy(&self) -> Box<dyn BoundCastData>;

    /// Returns self as Any for downcasting.
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Bound information for a type conversion.
#[derive(Clone)]
pub struct BoundCastInfo {
    /// The dispatch chosen during binding.
    pub dispatch: CastDispatch,
    /// Optional metadata for the cast (e.g. decimal precision/scale)
    pub cast_data: Option<Arc<dyn BoundCastData>>,
    context_dependency: CastContextDependency,
}

impl BoundCastInfo {
    pub fn fixed(function: FixedCastFn) -> Self {
        Self {
            dispatch: CastDispatch::Fixed(function),
            cast_data: None,
            context_dependency: CastContextDependency::Independent,
        }
    }

    pub fn varlen(function: VarlenCastFn) -> Self {
        Self {
            dispatch: CastDispatch::Varlen(function),
            cast_data: None,
            context_dependency: CastContextDependency::Independent,
        }
    }

    pub fn array(function: ArrayCastFn) -> Self {
        Self {
            dispatch: CastDispatch::Array(function),
            cast_data: None,
            context_dependency: CastContextDependency::Independent,
        }
    }

    pub fn structure(function: StructCastFn) -> Self {
        Self {
            dispatch: CastDispatch::Struct(function),
            cast_data: None,
            context_dependency: CastContextDependency::Independent,
        }
    }

    pub fn fixed_with_data(function: FixedCastFn, data: Arc<dyn BoundCastData>) -> Self {
        Self {
            dispatch: CastDispatch::Fixed(function),
            cast_data: Some(data),
            context_dependency: CastContextDependency::Independent,
        }
    }

    pub fn varlen_with_data(function: VarlenCastFn, data: Arc<dyn BoundCastData>) -> Self {
        Self {
            dispatch: CastDispatch::Varlen(function),
            cast_data: Some(data),
            context_dependency: CastContextDependency::Independent,
        }
    }

    pub fn array_with_data(function: ArrayCastFn, data: Arc<dyn BoundCastData>) -> Self {
        Self {
            dispatch: CastDispatch::Array(function),
            cast_data: Some(data),
            context_dependency: CastContextDependency::Independent,
        }
    }

    pub fn struct_with_data(function: StructCastFn, data: Arc<dyn BoundCastData>) -> Self {
        Self {
            dispatch: CastDispatch::Struct(function),
            cast_data: Some(data),
            context_dependency: CastContextDependency::Independent,
        }
    }

    /// Marks a cast as requiring the query runtime context for correct evaluation.
    pub fn requiring_runtime_context(mut self) -> Self {
        self.context_dependency = CastContextDependency::Runtime;
        self
    }

    /// Propagates the context requirement of a nested cast.
    pub fn with_context_dependency(mut self, dependency: CastContextDependency) -> Self {
        self.context_dependency = self.context_dependency.combine(dependency);
        self
    }

    pub fn context_dependency(&self) -> CastContextDependency {
        self.context_dependency
    }

    pub fn identity(source: &LogicalType, target: &LogicalType) -> Self {
        match source.physical_type() {
            PhysicalType::Bool
            | PhysicalType::Int8
            | PhysicalType::Int16
            | PhysicalType::Int32
            | PhysicalType::Int64
            | PhysicalType::Int128
            | PhysicalType::UInt8
            | PhysicalType::UInt16
            | PhysicalType::UInt32
            | PhysicalType::UInt64
            | PhysicalType::UInt128
            | PhysicalType::Float
            | PhysicalType::Double => Self::fixed(reference_identity_cast),
            PhysicalType::Varchar => Self::varlen(reference_identity_cast),
            PhysicalType::Array | PhysicalType::List => Self::array(reference_identity_cast),
            PhysicalType::Struct => Self::structure(reference_identity_cast),
            PhysicalType::Bit => {
                panic!("bit cast identity is not implemented for {source} -> {target}")
            }
        }
    }

    pub fn null(target: &LogicalType) -> Self {
        match target.physical_type() {
            PhysicalType::Bool
            | PhysicalType::Int8
            | PhysicalType::Int16
            | PhysicalType::Int32
            | PhysicalType::Int64
            | PhysicalType::Int128
            | PhysicalType::UInt8
            | PhysicalType::UInt16
            | PhysicalType::UInt32
            | PhysicalType::UInt64
            | PhysicalType::UInt128
            | PhysicalType::Float
            | PhysicalType::Double => Self::fixed(null_cast),
            PhysicalType::Varchar => Self::varlen(null_cast),
            PhysicalType::Array | PhysicalType::List => Self::array(null_cast),
            PhysicalType::Struct => Self::structure(null_cast),
            PhysicalType::Bit => panic!("null cast is not implemented for {target}"),
        }
    }

    pub fn execute(
        &self,
        source: &Vector,
        result: &mut Vector,
        count: usize,
        ctx: &CastExecCtx<'_>,
    ) -> Result<bool> {
        match self.dispatch {
            CastDispatch::Fixed(function) => function(source, result, count, ctx),
            CastDispatch::Varlen(function) => function(source, result, count, ctx),
            CastDispatch::Array(function) => function(source, result, count, ctx),
            CastDispatch::Struct(function) => function(source, result, count, ctx),
        }
    }
}

/// CastFunctionSet manages the registration and lookup of type conversion functions.
pub struct CastFunctionSet {
    /// Exact type matches (Source -> Target)
    direct_casts: HashMap<(LogicalType, LogicalType), BoundCastInfo>,
    /// Dynamic binding functions for complex types (e.g. Decimals, Lists)
    bind_functions: Vec<BindCastFunction>,
}

impl Default for CastFunctionSet {
    fn default() -> Self {
        Self::new()
    }
}

impl CastFunctionSet {
    pub fn new() -> Self {
        Self {
            direct_casts: HashMap::new(),
            bind_functions: Vec::new(),
        }
    }

    /// Register a direct cast from source to target.
    pub fn register_cast(&mut self, source: LogicalType, target: LogicalType, info: BoundCastInfo) {
        self.direct_casts.insert((source, target), info);
    }

    /// Register a dynamic binding function.
    pub fn register_bind_function(&mut self, function: BindCastFunctionFn) {
        self.bind_functions.push(BindCastFunction { function });
    }

    /// Lookup a cast function from source to target.
    pub fn get_cast_function(
        &self,
        source: &LogicalType,
        target: &LogicalType,
    ) -> Result<BoundCastInfo> {
        // 1. Check for NopCast (same type)
        if source == target {
            return Ok(BoundCastInfo::identity(source, target));
        }

        // 2. Check direct casts
        if let Some(info) = self.direct_casts.get(&(source.clone(), target.clone())) {
            return Ok(info.clone());
        }

        // 3. Check dynamic bind functions (in reverse order of registration)
        let bind_input = BindCastInput::new(self);
        for (_i, bind_func) in self.bind_functions.iter().enumerate().rev() {
            if let Some(info) = (bind_func.function)(&bind_input, source, target)? {
                return Ok(info);
            }
        }

        Err(paro_error::cannot_cast(
            source.to_string(),
            target.to_string(),
        ))
    }
}

impl fmt::Debug for CastFunctionSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CastFunctionSet")
            .field("direct_casts_count", &self.direct_casts.len())
            .field("bind_functions_count", &self.bind_functions.len())
            .finish()
    }
}

impl fmt::Debug for BoundCastInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundCastInfo")
            .field("dispatch", &self.dispatch)
            .field("cast_data", &self.cast_data)
            .field("context_dependency", &self.context_dependency)
            .finish()
    }
}

impl fmt::Debug for CastDispatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fixed(function) => f
                .debug_tuple("Fixed")
                .field(&format_args!("{:p}", *function as *const ()))
                .finish(),
            Self::Varlen(function) => f
                .debug_tuple("Varlen")
                .field(&format_args!("{:p}", *function as *const ()))
                .finish(),
            Self::Array(function) => f
                .debug_tuple("Array")
                .field(&format_args!("{:p}", *function as *const ()))
                .finish(),
            Self::Struct(function) => f
                .debug_tuple("Struct")
                .field(&format_args!("{:p}", *function as *const ()))
                .finish(),
        }
    }
}

fn reference_identity_cast(
    source: &Vector,
    result: &mut Vector,
    count: usize,
    _ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    let target_type = result.logical_type().clone();
    *result = source.reference_as(target_type);
    result.set_count(count);
    Ok(true)
}

fn null_cast(
    _source: &Vector,
    result: &mut Vector,
    count: usize,
    _ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    result.set_count(count);
    result.validity_mut().set_all_invalid(count);
    Ok(true)
}
