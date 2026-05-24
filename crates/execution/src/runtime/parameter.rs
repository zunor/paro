// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Runtime parameter bindings for reusable program images.

use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::runtime_value::Value;
use paro_common::typed_parameters::{ParameterSlot, RuntimeParamId, TypedParameterEnv};
use paro_common::types::LogicalType;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct ParameterBindingEpoch(u64);

impl ParameterBindingEpoch {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    #[inline]
    pub fn value(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParameterBindings {
    values: Box<[Value]>,
    types: Box<[LogicalType]>,
    epoch: ParameterBindingEpoch,
}

impl ParameterBindings {
    pub fn new(
        values: Vec<Value>,
        types: Vec<LogicalType>,
        epoch: ParameterBindingEpoch,
    ) -> Result<Self> {
        if values.len() != types.len() {
            return Err(paro_error::internal(format!(
                "parameter value/type length mismatch: values={}, types={}",
                values.len(),
                types.len()
            )));
        }
        for (idx, (value, ty)) in values.iter().zip(types.iter()).enumerate() {
            if &value.logical_type() != ty {
                return Err(paro_error::internal(format!(
                    "parameter {idx} type mismatch: value={:?}, expected={:?}",
                    value.logical_type(),
                    ty
                )));
            }
        }
        Ok(Self {
            values: values.into_boxed_slice(),
            types: types.into_boxed_slice(),
            epoch,
        })
    }

    pub fn empty() -> Self {
        Self {
            values: Box::new([]),
            types: Box::new([]),
            epoch: ParameterBindingEpoch::default(),
        }
    }

    pub fn from_typed_env(env: &TypedParameterEnv, epoch: ParameterBindingEpoch) -> Self {
        Self {
            values: env.iter().map(|param| param.value.clone()).collect(),
            types: env.iter().map(|param| param.logical_type.clone()).collect(),
            epoch,
        }
    }

    #[inline]
    pub fn epoch(&self) -> ParameterBindingEpoch {
        self.epoch
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn value(&self, id: RuntimeParamId) -> Option<&Value> {
        self.values.get(id.index())
    }

    pub fn logical_type(&self, id: RuntimeParamId) -> Option<&LogicalType> {
        self.types.get(id.index())
    }

    pub fn value_for_slot(&self, slot: &ParameterSlot) -> Result<&Value> {
        let Some(value) = self.value(slot.index) else {
            return Err(paro_error::internal(format!(
                "parameter slot {} is not bound",
                slot.index.index()
            )));
        };
        let Some(bound_type) = self.logical_type(slot.index) else {
            return Err(paro_error::internal(format!(
                "parameter slot {} has no type",
                slot.index.index()
            )));
        };
        if bound_type != &slot.ty {
            return Err(paro_error::internal(format!(
                "parameter slot {} type changed within execution: bound={bound_type:?}, slot={:?}",
                slot.index.index(),
                slot.ty
            )));
        }
        Ok(value)
    }
}

pub struct ExpressionEvalInput<'a> {
    pub params: &'a ParameterBindings,
    pub columns: &'a Chunk,
}

#[cfg(test)]
mod tests {
    use paro_common::runtime_value::Value;
    use paro_common::typed_parameters::{ParameterSlot, RuntimeParamId};
    use paro_common::types::LogicalType;

    use super::*;

    #[test]
    fn value_for_slot_rejects_type_mismatch() {
        let bindings = ParameterBindings::new(
            vec![Value::Integer(7)],
            vec![LogicalType::Integer],
            ParameterBindingEpoch::new(1),
        )
        .expect("bindings should be valid");
        let slot = ParameterSlot::new(RuntimeParamId::new(0), LogicalType::BigInt);

        let err = bindings
            .value_for_slot(&slot)
            .expect_err("slot type mismatch should fail");

        assert!(err.to_string().contains("type changed"));
    }
}
