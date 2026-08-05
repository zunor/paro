// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Struct Cast Functions
//!
//! Implements Struct -> Struct casting by casting each field by position.

use std::sync::Arc;

use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_common::vector::Vector;

use super::{BindCastInput, BoundCastData, BoundCastInfo, CastContextDependency, CastExecCtx};

/// Bound cast data for Struct casts.
#[derive(Debug)]
pub struct StructBoundCastData {
    pub field_casts: Vec<BoundCastInfo>,
}

impl BoundCastData for StructBoundCastData {
    fn copy(&self) -> Box<dyn BoundCastData> {
        Box::new(Self {
            field_casts: self.field_casts.clone(),
        })
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Cast Struct -> Struct by casting each field.
pub fn struct_to_struct_cast(
    source: &Vector,
    result: &mut Vector,
    count: usize,
    ctx: &CastExecCtx<'_>,
) -> Result<bool> {
    let source_fields = match source.logical_type() {
        LogicalType::Struct(fields) => fields,
        _ => {
            return Err(paro_error::internal(
                "struct_to_struct_cast: source is not Struct type",
            ))
        }
    };
    let target_fields = match result.logical_type() {
        LogicalType::Struct(fields) => fields,
        _ => {
            return Err(paro_error::internal(
                "struct_to_struct_cast: result is not Struct type",
            ))
        }
    };

    if source_fields.len() != target_fields.len() {
        return Err(paro_error::type_mismatch(format!(
            "Struct field count mismatch: {} vs {}",
            source_fields.len(),
            target_fields.len()
        )));
    }

    let cast_data = ctx
        .cast_data
        .ok_or_else(|| paro_error::internal("struct_to_struct_cast: missing cast_data"))?;
    let struct_cast_data = cast_data
        .as_any()
        .downcast_ref::<StructBoundCastData>()
        .ok_or_else(|| paro_error::internal("struct_to_struct_cast: invalid cast_data"))?;

    let source_children = source
        .children()
        .ok_or_else(|| paro_error::internal("struct_to_struct_cast: missing source children"))?;

    result.set_count(count);
    result.validity_mut().copy(source.validity(), count);

    let result_children = result
        .children_mut()
        .ok_or_else(|| paro_error::internal("struct_to_struct_cast: missing result children"))?;

    if source_children.len() != result_children.len() {
        return Err(paro_error::internal(
            "struct_to_struct_cast: child vector count mismatch",
        ));
    }

    let mut all_success = true;
    for (idx, cast_info) in struct_cast_data.field_casts.iter().enumerate() {
        let child_ctx = CastExecCtx {
            runtime: ctx.runtime,
            try_cast: ctx.try_cast,
            cast_data: cast_info.cast_data.as_ref().map(|d| d.as_ref()),
        };

        let src_child = &source_children[idx];
        let dst_child = Arc::make_mut(&mut result_children[idx]);
        let success = cast_info.execute(src_child, dst_child, count, &child_ctx)?;
        all_success &= success;
    }

    Ok(all_success)
}

/// Bind function for Struct casts.
pub fn bind_struct_casts(
    input: &BindCastInput,
    source: &LogicalType,
    target: &LogicalType,
) -> Result<Option<BoundCastInfo>> {
    let (LogicalType::Struct(source_fields), LogicalType::Struct(target_fields)) = (source, target)
    else {
        return Ok(None);
    };

    if source_fields.len() != target_fields.len() {
        return Err(paro_error::type_mismatch(format!(
            "Struct field count mismatch: {} vs {}",
            source_fields.len(),
            target_fields.len()
        )));
    }

    let mut field_casts = Vec::with_capacity(source_fields.len());
    let mut dependency = CastContextDependency::Independent;
    for ((_, source_ty), (_, target_ty)) in source_fields.iter().zip(target_fields.iter()) {
        let cast_info = input.get_cast_function(source_ty, target_ty)?;
        dependency = dependency.combine(cast_info.context_dependency());
        field_casts.push(cast_info);
    }

    let data = StructBoundCastData { field_casts };
    Ok(Some(
        BoundCastInfo::struct_with_data(struct_to_struct_cast, Arc::new(data))
            .with_context_dependency(dependency),
    ))
}
