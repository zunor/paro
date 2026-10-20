// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Exact routine revisions and trusted Arrow schemas sent to the worker.

use std::sync::Arc;

use arrow_schema::Schema;
use base64::{engine::general_purpose::STANDARD, Engine};
use paro_common::error::{self as error, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::codec;
use crate::routine::spec::{
    PythonEntrypointRef, RoutineImplementationRef, RoutineNullPolicy, RoutineReturn, RoutineSpec,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Definition {
    pub routine_id: u64,
    pub generation: u64,
    pub owner: String,
    pub source: String,
    pub digest: String,
    pub handler: String,
    pub mode: String,
    pub strict: bool,
    pub input_schema: String,
    pub output_schema: String,
}

pub fn source_digest(source: &str) -> String {
    format!("{:x}", Sha256::digest(source.as_bytes()))
}

pub fn schema_wire(schema: &Schema) -> Result<String> {
    let options = arrow_ipc::writer::IpcWriteOptions::default();
    let data = arrow_ipc::writer::IpcDataGenerator::default()
        .schema_to_bytes_with_dictionary_tracker(
            schema,
            &mut arrow_ipc::writer::DictionaryTracker::new(false),
            &options,
        );
    let mut wire = Vec::new();
    arrow_ipc::writer::write_message(&mut wire, data, &options)
        .map_err(|e| error::external_protocol_mismatch(e.to_string()))?;
    Ok(STANDARD.encode(wire))
}

impl Definition {
    pub fn from_spec(spec: &RoutineSpec) -> Result<(Self, Arc<Schema>, Arc<Schema>)> {
        let RoutineImplementationRef::Python(implementation) = &spec.implementation;
        let (handler, mode) = match &implementation.entrypoint {
            PythonEntrypointRef::Scalar { handler } => (handler, "scalar"),
            PythonEntrypointRef::Vector { handler } => (handler, "vector"),
            _ => {
                return Err(error::not_supported(
                    "Python aggregate/window execution requires an explicit state contract",
                ))
            }
        };
        let input = codec::schema(
            &spec
                .arguments
                .iter()
                .enumerate()
                .map(|(index, arg)| (format!("arg{index}"), arg.data_type.clone()))
                .collect::<Vec<_>>(),
        )?;
        let (output, mode) = match &spec.return_type {
            RoutineReturn::Scalar(ty) => (codec::schema(&[("result".into(), ty.clone())])?, mode),
            RoutineReturn::Table(fields) => (
                codec::schema(
                    &fields
                        .iter()
                        .map(|field| (field.name.clone(), field.data_type.clone()))
                        .collect::<Vec<_>>(),
                )?,
                "table",
            ),
        };
        let source = implementation.source_blob.inline_source.clone();
        Ok((
            Self {
                routine_id: spec.identity.id.raw(),
                generation: spec.identity.generation,
                owner: spec.owner.principal.clone(),
                digest: source_digest(&source),
                source,
                handler: handler.clone(),
                mode: mode.into(),
                strict: spec.semantics.null_policy == RoutineNullPolicy::Strict,
                input_schema: schema_wire(&input)?,
                output_schema: schema_wire(&output)?,
            },
            input,
            output,
        ))
    }

    pub fn fence(&self) -> serde_json::Value {
        serde_json::json!([self.routine_id, self.generation, self.digest])
    }
}
