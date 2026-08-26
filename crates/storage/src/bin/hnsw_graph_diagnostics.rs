// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Offline, governed structural qualification for one durable HNSW generation.

use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use paro_storage::index::hnsw::HnswGraphDiagnostics;
use paro_storage::search::{qualify_hnsw_generation, ResourceBudget};

const DEFAULT_MEMORY_MIB: usize = 512;

struct Arguments {
    table_data_dir: PathBuf,
    definition_id: u64,
    memory_mib: usize,
    indegrees_path: Option<PathBuf>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = parse_arguments()?;
    let memory_bytes = arguments
        .memory_mib
        .checked_mul(1024 * 1024)
        .ok_or("diagnostic memory limit overflows usize")?;
    let budget = ResourceBudget::standalone(memory_bytes, usize::MAX, 1);
    let started_at = Instant::now();
    let qualification =
        qualify_hnsw_generation(&arguments.table_data_dir, arguments.definition_id, &budget)?;
    let report = qualification.diagnostics.report();
    let elapsed_seconds = started_at.elapsed().as_secs_f64();

    if let Some(path) = arguments.indegrees_path.as_deref() {
        write_indegrees(path, qualification.diagnostics.indegrees())?;
    }
    let has_indegree_output = arguments.indegrees_path.is_some();

    let output = serde_json::json!({
        "schema_version": 1,
        "definition_id": qualification.definition_id,
        "generation_id": qualification.generation_id,
        "column_id": qualification.column_id,
        "artifact_format_version": qualification.artifact_format_version,
        "coverage": qualification.coverage,
        "report": report,
        "estimated_peak_memory_bytes": HnswGraphDiagnostics::estimated_peak_memory_bytes(
            usize::try_from(report.point_count)?
        )?,
        "elapsed_seconds": elapsed_seconds,
        "indegrees_path": arguments.indegrees_path.as_ref(),
        "indegrees_encoding": has_indegree_output.then_some("raw-little-endian-u32-v1"),
    });
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn parse_arguments() -> Result<Arguments, Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let table_data_dir = args.next().map(PathBuf::from).ok_or_else(usage)?;
    let definition_id = args
        .next()
        .ok_or_else(usage)?
        .parse::<u64>()
        .map_err(|error| format!("invalid definition id: {error}"))?;
    let mut memory_mib = DEFAULT_MEMORY_MIB;
    let mut indegrees_path = None;
    while let Some(option) = args.next() {
        match option.as_str() {
            "--memory-mib" => {
                memory_mib = args
                    .next()
                    .ok_or("--memory-mib requires a value")?
                    .parse::<usize>()
                    .map_err(|error| format!("invalid --memory-mib value: {error}"))?;
                if memory_mib == 0 {
                    return Err("--memory-mib must be positive".into());
                }
            }
            "--indegrees" => {
                indegrees_path = Some(PathBuf::from(
                    args.next().ok_or("--indegrees requires a path")?,
                ));
            }
            _ => return Err(format!("unknown option {option}\n{}", usage()).into()),
        }
    }
    Ok(Arguments {
        table_data_dir,
        definition_id,
        memory_mib,
        indegrees_path,
    })
}

fn write_indegrees(path: &Path, indegrees: &[u32]) -> Result<(), Box<dyn std::error::Error>> {
    let mut output = BufWriter::new(File::create(path)?);
    let mut bytes = Vec::with_capacity(64 * 1024);
    for chunk in indegrees.chunks((64 * 1024) / std::mem::size_of::<u32>()) {
        bytes.clear();
        for degree in chunk {
            bytes.extend_from_slice(&degree.to_le_bytes());
        }
        output.write_all(&bytes)?;
    }
    output.flush()?;
    Ok(())
}

fn usage() -> String {
    "usage: hnsw_graph_diagnostics <table-data-dir> <definition-id> [--memory-mib N] [--indegrees PATH]".to_string()
}
