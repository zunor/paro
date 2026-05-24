// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::runtime_value::Value;
use crate::types::LogicalType;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RuntimeParamId(u32);

impl RuntimeParamId {
    pub fn new(index: usize) -> Self {
        assert!(index <= u32::MAX as usize, "runtime parameter id exhausted");
        Self(index as u32)
    }

    #[inline]
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParameterSlot {
    pub index: RuntimeParamId,
    pub ty: LogicalType,
}

impl ParameterSlot {
    pub fn new(index: RuntimeParamId, ty: LogicalType) -> Self {
        Self { index, ty }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundParameter {
    pub value: Value,
    pub logical_type: LogicalType,
}

impl BoundParameter {
    pub fn new(value: Value, logical_type: LogicalType) -> Self {
        Self {
            value,
            logical_type,
        }
    }

    pub fn null(logical_type: LogicalType) -> Self {
        Self {
            value: Value::Null(logical_type.clone()),
            logical_type,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TypedParameterEnv {
    params: Vec<BoundParameter>,
}

impl TypedParameterEnv {
    pub fn new(params: Vec<BoundParameter>) -> Self {
        Self { params }
    }

    pub fn is_empty(&self) -> bool {
        self.params.is_empty()
    }

    pub fn len(&self) -> usize {
        self.params.len()
    }

    pub fn get(&self, index: usize) -> Option<&BoundParameter> {
        self.params.get(index)
    }

    pub fn iter(&self) -> impl Iterator<Item = &BoundParameter> {
        self.params.iter()
    }

    pub fn params(&self) -> &[BoundParameter] {
        &self.params
    }

    pub fn values(&self) -> Vec<Value> {
        self.params
            .iter()
            .map(|param| param.value.clone())
            .collect()
    }

    pub fn logical_types(&self) -> Vec<Option<LogicalType>> {
        self.params
            .iter()
            .map(|param| Some(param.logical_type.clone()))
            .collect()
    }
}
