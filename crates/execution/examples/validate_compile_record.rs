// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Current typed Summary schema validator. Does not certify cross-run association.
use std::io::Read;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: validate_compile_record FILE")?;
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(paro_context::compile_diagnostics::ENCODED_LIMIT as u64 + 1)
        .read_to_end(&mut bytes)?;
    let record = paro_execution::explain::compile_render::validate_json(&bytes)?;
    match record {
        paro_context::compile_diagnostics::CompileDocument::Summary(record) => println!(
            "schema={} invocation={} outcome={:?} (schema validation only)",
            record.schema_version, record.invocation, record.outcome
        ),
        paro_context::compile_diagnostics::CompileDocument::Unavailable(record) => println!(
            "schema={} unavailable={:?} (schema validation only)",
            record.schema_version, record.reason
        ),
    }
    Ok(())
}
