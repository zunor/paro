// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::cast_rules::CastRules;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use serde::{Deserialize, Serialize};

use crate::env::{DeclaredEnvSpec, PythonRuntimeSelector};
use crate::permission::PermissionSpec;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct RoutineId(pub u64);

impl RoutineId {
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    pub const fn raw(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutineIdentity {
    pub id: RoutineId,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutineOwner {
    pub principal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutineArgument {
    pub name: Option<String>,
    pub data_type: LogicalType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutineTableColumn {
    pub name: String,
    pub data_type: LogicalType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoutineFamily {
    ScalarBatch,
    TableBatch,
    AggregateBatch,
    WindowBatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutineSemantics {
    pub stability: RoutineStability,
    pub null_policy: RoutineNullPolicy,
    pub side_effects: RoutineSideEffects,
    pub row_semantics: RowSemantics,
    pub may_block: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoutineStability {
    Immutable,
    Stable,
    Volatile,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoutineNullPolicy {
    Strict,
    CalledOnNullInput,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoutineSideEffects {
    None,
    HasSideEffects,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RowSemantics {
    RowPreserving,
    RelationExpanding,
    Aggregate,
    Window,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoutineReturn {
    Scalar(LogicalType),
    Table(Vec<RoutineTableColumn>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoutineExecutionContract {
    Scalar(ScalarRoutineContract),
    Table(TableRoutineContract),
    Aggregate(AggregateRoutineContract),
    Window(WindowRoutineContract),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ScalarRoutineContract;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableRoutineContract {
    pub rows_hint: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateRoutineContract {
    pub state_abi: AggregateStateAbi,
    pub supports_partial: bool,
    pub supports_combine: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregateStateAbi {
    FixedLayoutStruct,
    ArrowAdjacentStruct,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowRoutineContract {
    pub requires_partition_order_materialization: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoutineImplementationRef {
    Python(PythonImplementationRef),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceBlobRef {
    pub id: String,
    pub inline_source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PythonImplementationRef {
    pub source_blob: SourceBlobRef,
    pub entrypoint: PythonEntrypointRef,
    pub runtime: PythonRuntimeSelector,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PythonEntrypointRef {
    Batch {
        handler: String,
    },
    Aggregate {
        init: String,
        update: String,
        combine: Option<String>,
        finalize: String,
    },
    Window {
        handler: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutineSpec {
    pub identity: RoutineIdentity,
    pub name: String,
    pub schema: String,
    pub owner: RoutineOwner,
    pub arguments: Vec<RoutineArgument>,
    pub family: RoutineFamily,
    pub return_type: RoutineReturn,
    pub execution_contract: RoutineExecutionContract,
    pub semantics: RoutineSemantics,
    pub implementation: RoutineImplementationRef,
    pub environment: DeclaredEnvSpec,
    pub permissions: PermissionSpec,
}

impl RoutineSpec {
    pub fn signature(&self) -> RoutineSignature {
        RoutineSignature {
            argument_types: self
                .arguments
                .iter()
                .map(|arg| arg.data_type.clone())
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutineSignature {
    pub argument_types: Vec<LogicalType>,
}

impl RoutineSignature {
    pub fn exact_match(&self, argument_types: &[LogicalType]) -> bool {
        self.argument_types == argument_types
    }
}

pub fn resolve_best_match<'a>(
    candidates: impl IntoIterator<Item = &'a RoutineSpec>,
    arguments: &[LogicalType],
) -> Result<&'a RoutineSpec> {
    let mut best_match: Option<(&RoutineSpec, i64)> = None;

    for candidate in candidates {
        let signature = candidate.signature();
        if signature.argument_types.len() != arguments.len() {
            continue;
        }

        let mut total_cost = 0i64;
        let mut valid = true;
        for (actual, expected) in arguments.iter().zip(signature.argument_types.iter()) {
            let cost = CastRules::implicit_cast_cost(actual, expected);
            if cost < 0 {
                valid = false;
                break;
            }
            total_cost += cost;
        }
        if !valid {
            continue;
        }

        match best_match {
            None => best_match = Some((candidate, total_cost)),
            Some((_, best_cost)) if total_cost < best_cost => {
                best_match = Some((candidate, total_cost));
            }
            _ => {}
        }
    }

    best_match
        .map(|(spec, _)| spec)
        .ok_or_else(|| paro_error::function_not_found("routine overload not found"))
}
