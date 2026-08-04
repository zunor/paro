// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Immutable compiled statement images and per-execution inputs.

use std::sync::Arc;

use paro_common::error::{self as paro_error, Result};
use paro_common::typed_parameters::TypedParameterEnv;
use paro_common::types::LogicalType;
use paro_context::CompileEnvironmentKey;

use crate::pipeline::StatementProgram;
use crate::runtime::{ParameterBindingEpoch, ParameterBindings};

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

/// A shareable, immutable program image produced by the compiler.
///
/// Runtime parameter values deliberately do not live here. Cloning a compiled
/// statement is therefore an O(1) operation suitable for prepared-plan caches.
#[derive(Debug, Clone)]
pub struct CompiledStatement {
    image: Arc<CompiledStatementImage>,
}

#[derive(Debug)]
struct CompiledStatementImage {
    executable: CompiledExecutable,
    result_schema: Box<[ResultColumnDesc]>,
    parameter_types: Box<[LogicalType]>,
    compile_environment: CompileEnvironmentKey,
}

#[derive(Debug)]
pub enum CompiledExecutable {
    Program(StatementProgram),
}

impl CompiledStatement {
    pub fn new(
        program: StatementProgram,
        result_schema: Vec<ResultColumnDesc>,
        parameter_types: Vec<LogicalType>,
        compile_environment: CompileEnvironmentKey,
    ) -> Self {
        Self {
            image: Arc::new(CompiledStatementImage {
                executable: CompiledExecutable::Program(program),
                result_schema: result_schema.into_boxed_slice(),
                parameter_types: parameter_types.into_boxed_slice(),
                compile_environment,
            }),
        }
    }

    #[inline]
    pub fn executable(&self) -> &CompiledExecutable {
        &self.image.executable
    }

    #[inline]
    pub fn result_schema(&self) -> &[ResultColumnDesc] {
        &self.image.result_schema
    }

    #[inline]
    pub fn parameter_types(&self) -> &[LogicalType] {
        &self.image.parameter_types
    }

    #[inline]
    pub fn compile_environment(&self) -> &CompileEnvironmentKey {
        &self.image.compile_environment
    }

    #[inline]
    pub fn is_query(&self) -> bool {
        !self.image.result_schema.is_empty()
    }

    #[inline]
    pub fn column_count(&self) -> usize {
        self.image.result_schema.len()
    }

    pub fn result_names(&self) -> Vec<String> {
        self.image
            .result_schema
            .iter()
            .map(|col| col.name.clone())
            .collect()
    }

    pub fn result_types(&self) -> Vec<LogicalType> {
        self.image
            .result_schema
            .iter()
            .map(|col| col.logical_type.clone())
            .collect()
    }

    /// Returns true when both handles point at the same compiled image.
    #[inline]
    pub fn shares_image_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.image, &other.image)
    }
}

/// A single execution of a compiled statement with query-local bindings.
///
/// This is the only accepted executor input, keeping parameter values out of
/// prepared-plan caches and making the plan/value lifetime boundary explicit.
#[derive(Debug, Clone)]
pub struct ExecutionRequest {
    statement: CompiledStatement,
    bindings: Arc<ParameterBindings>,
}

impl ExecutionRequest {
    pub fn new(statement: CompiledStatement, bindings: ParameterBindings) -> Result<Self> {
        Self::validate_bindings(&statement, &bindings)?;
        Ok(Self {
            statement,
            bindings: Arc::new(bindings),
        })
    }

    fn validate_bindings(
        statement: &CompiledStatement,
        bindings: &ParameterBindings,
    ) -> Result<()> {
        if statement.parameter_types().len() != bindings.len() {
            return Err(paro_error::protocol_violation(format!(
                "compiled statement expects {} parameters, but execution supplied {}",
                statement.parameter_types().len(),
                bindings.len()
            )));
        }
        for (index, expected) in statement.parameter_types().iter().enumerate() {
            let actual = bindings
                .logical_type(paro_common::typed_parameters::RuntimeParamId::new(index))
                .expect("parameter count checked");
            if actual != expected {
                return Err(paro_error::protocol_violation(format!(
                    "parameter {} has type {}, but compiled statement expects {}",
                    index + 1,
                    actual,
                    expected
                )));
            }
        }
        Ok(())
    }

    pub fn unparameterized(statement: CompiledStatement) -> Result<Self> {
        Self::new(statement, ParameterBindings::empty())
    }

    pub fn from_typed_env(
        statement: CompiledStatement,
        parameter_env: &TypedParameterEnv,
    ) -> Result<Self> {
        Self::new(
            statement,
            ParameterBindings::from_typed_env(parameter_env, ParameterBindingEpoch::new(1)),
        )
    }

    /// Replaces a stale compiled image while retaining this execution's bindings.
    ///
    /// Revalidation is intentional here: catalog changes can cause recompilation
    /// to produce a different parameter signature even when the SQL is unchanged.
    pub fn with_statement(self, statement: CompiledStatement) -> Result<Self> {
        Self::validate_bindings(&statement, &self.bindings)?;
        Ok(Self {
            statement,
            bindings: self.bindings,
        })
    }

    #[inline]
    pub fn statement(&self) -> &CompiledStatement {
        &self.statement
    }

    pub fn into_parts(self) -> (CompiledStatement, Arc<ParameterBindings>) {
        (self.statement, self.bindings)
    }
}

#[cfg(test)]
mod tests {
    use paro_common::runtime_value::Value;
    use paro_context::TestStatementContextBuilder;

    use super::*;
    use crate::physical::{UnsupportedUtilitySpec, UtilitySpec};
    use crate::pipeline::{StatementProgram, UtilityProgram};

    fn statement(parameter_types: Vec<LogicalType>) -> CompiledStatement {
        CompiledStatement::new(
            StatementProgram::Utility(UtilityProgram {
                spec: UtilitySpec::Unsupported(UnsupportedUtilitySpec {
                    name: "test".to_string(),
                }),
            }),
            Vec::new(),
            parameter_types,
            TestStatementContextBuilder::minimal()
                .build()
                .compile_environment_key(),
        )
    }

    #[test]
    fn compiled_statement_clones_share_the_program_image() {
        let compiled = statement(Vec::new());
        let cloned = compiled.clone();

        assert!(compiled.shares_image_with(&cloned));
    }

    #[test]
    fn execution_request_rejects_binding_type_changes() {
        let compiled = statement(vec![LogicalType::Integer]);
        let bindings = ParameterBindings::new(
            vec![Value::BigInt(7)],
            vec![LogicalType::BigInt],
            ParameterBindingEpoch::new(1),
        )
        .unwrap();

        let error = ExecutionRequest::new(compiled, bindings).unwrap_err();

        assert!(error.to_string().contains("parameter 1 has type"));
        assert!(error.to_string().contains("expects INTEGER"));
    }

    #[test]
    fn execution_request_reuses_bindings_when_replacing_a_stale_statement() {
        let bindings = ParameterBindings::new(
            vec![Value::Integer(7)],
            vec![LogicalType::Integer],
            ParameterBindingEpoch::new(1),
        )
        .unwrap();
        let request = ExecutionRequest::new(statement(vec![LogicalType::Integer]), bindings)
            .expect("matching bindings");
        let original_bindings = request.clone().into_parts().1;
        let replacement = statement(vec![LogicalType::Integer]);

        let replaced = request
            .with_statement(replacement.clone())
            .expect("matching replacement signature");
        let (replaced_statement, replaced_bindings) = replaced.into_parts();

        assert!(replaced_statement.shares_image_with(&replacement));
        assert!(Arc::ptr_eq(&original_bindings, &replaced_bindings));
    }
}
