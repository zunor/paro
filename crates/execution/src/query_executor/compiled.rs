// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::types::LogicalType;

use crate::pipeline::StatementProgram;
use crate::runtime::ParameterBindings;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultColumnDesc {
    pub name: String,
    pub logical_type: LogicalType,
}

impl ResultColumnDesc {
    pub fn new(name: impl Into<String>, logical_type: LogicalType) -> Self {
        Self {
            name: name.into(),
            logical_type,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompiledStatement {
    pub executable: CompiledExecutable,
    pub result_schema: Vec<ResultColumnDesc>,
    pub parameter_types: Vec<LogicalType>,
    pub parameter_bindings: ParameterBindings,
}

#[derive(Debug, Clone)]
pub enum CompiledExecutable {
    Program(StatementProgram),
}

impl CompiledStatement {
    pub fn new(
        program: StatementProgram,
        result_schema: Vec<ResultColumnDesc>,
        parameter_types: Vec<LogicalType>,
    ) -> Self {
        Self::new_with_bindings(
            program,
            result_schema,
            parameter_types,
            ParameterBindings::empty(),
        )
    }

    pub fn new_with_bindings(
        program: StatementProgram,
        result_schema: Vec<ResultColumnDesc>,
        parameter_types: Vec<LogicalType>,
        parameter_bindings: ParameterBindings,
    ) -> Self {
        Self {
            executable: CompiledExecutable::Program(program),
            result_schema,
            parameter_types,
            parameter_bindings,
        }
    }

    #[inline]
    pub fn is_query(&self) -> bool {
        !self.result_schema.is_empty()
    }

    #[inline]
    pub fn column_count(&self) -> usize {
        self.result_schema.len()
    }

    pub fn result_names(&self) -> Vec<String> {
        self.result_schema
            .iter()
            .map(|col| col.name.clone())
            .collect()
    }

    pub fn result_types(&self) -> Vec<LogicalType> {
        self.result_schema
            .iter()
            .map(|col| col.logical_type.clone())
            .collect()
    }
}
