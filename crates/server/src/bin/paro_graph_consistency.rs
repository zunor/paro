use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

use paro_catalog::mvcc::CatalogSnapshot;
use paro_common::identity::GraphId;
use paro_execution::operator::ddl::property_graph_support::scan_graph_inputs_with_catalog;
use paro_instance::{Instance, InstanceConfig};
use paro_storage::index::graph::{GraphStatsProvider, NeighborView, VertexKey};

#[derive(Debug, Clone)]
struct Cli {
    data_dir: PathBuf,
    database: String,
    graph: Option<String>,
    sample: usize,
}

#[derive(Debug, Default)]
struct GraphCheckSummary {
    vertex_labels: usize,
    edge_labels: usize,
    vertices_checked: usize,
    edges_checked: usize,
    mismatches: Vec<String>,
}

fn usage() -> &'static str {
    "Usage: cargo run -p paro-server --bin paro_graph_consistency -- --data-dir <path> [--database postgres] [--graph <name>] [--sample 32]"
}

fn parse_args() -> Result<Cli, String> {
    let mut data_dir: Option<PathBuf> = None;
    let mut database = "postgres".to_string();
    let mut graph = None;
    let mut sample = 32usize;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--data-dir" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--data-dir requires a value".to_string())?;
                data_dir = Some(PathBuf::from(value));
            }
            "--database" => {
                database = args
                    .next()
                    .ok_or_else(|| "--database requires a value".to_string())?;
            }
            "--graph" => {
                graph = Some(
                    args.next()
                        .ok_or_else(|| "--graph requires a value".to_string())?,
                );
            }
            "--sample" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--sample requires a value".to_string())?;
                sample = value
                    .parse::<usize>()
                    .map_err(|_| format!("invalid --sample value: {}", value))?;
            }
            "--help" | "-h" => {
                return Err(usage().to_string());
            }
            other => {
                return Err(format!("unknown argument: {}\n{}", other, usage()));
            }
        }
    }

    let data_dir = data_dir.ok_or_else(|| format!("missing --data-dir\n{}", usage()))?;
    Ok(Cli {
        data_dir,
        database,
        graph,
        sample,
    })
}

fn sample_indices(len: usize, sample: usize) -> Vec<usize> {
    if len == 0 {
        return Vec::new();
    }
    if sample == 0 || sample >= len {
        return (0..len).collect();
    }
    let step = len.div_ceil(sample);
    let mut out = (0..len).step_by(step).take(sample).collect::<Vec<_>>();
    if out.last().copied() != Some(len - 1) {
        out.push(len - 1);
    }
    out.sort_unstable();
    out.dedup();
    out
}

fn format_vertex_key(key: &VertexKey) -> String {
    match key {
        VertexKey::Int64(v) => v.to_string(),
        VertexKey::String(v) => v.to_string(),
        VertexKey::Composite(parts) => format!("{:?}", parts),
    }
}

fn neighbor_contains(view: NeighborView<'_>, dst_local: u32, edge_rowid: u64) -> bool {
    (0..view.len()).any(|idx| {
        view.pair_at(idx)
            .map(|(neighbor, rowid)| neighbor == dst_local && rowid == edge_rowid)
            .unwrap_or(false)
    })
}

fn run() -> Result<(), String> {
    let cli = parse_args()?;
    let config =
        InstanceConfig::new().with_instance_root(cli.data_dir.to_string_lossy().to_string());
    let instance =
        Instance::new(config).map_err(|err| format!("failed to open instance: {}", err))?;
    let database = instance
        .database_registry()
        .get_database(&cli.database)
        .ok_or_else(|| format!("database not found: {}", cli.database))?;

    let txn = CatalogSnapshot::default();
    let mut graphs = database.catalog().scan_property_graphs(&txn);
    if let Some(graph_name) = &cli.graph {
        graphs.retain(|graph| graph.info.graph_name == *graph_name);
    }
    if graphs.is_empty() {
        return Err(match &cli.graph {
            Some(graph_name) => format!("property graph not found: {}", graph_name),
            None => "no property graphs found".to_string(),
        });
    }

    let mut failed = false;
    for graph in graphs {
        let scanned =
            scan_graph_inputs_with_catalog(database.catalog().as_ref(), &txn, &graph.info)
                .map_err(|err| {
                    format!(
                        "failed to scan graph inputs for {}: {}",
                        graph.info.graph_name, err
                    )
                })?;
        let snapshot = instance
            .graph_manager()
            .snapshot(
                &GraphId::new(&cli.database, &graph.info.schema, &graph.info.graph_name)
                    .runtime_key(),
            )
            .ok_or_else(|| format!("graph runtime snapshot missing: {}", graph.info.graph_name))?;
        let mut summary = GraphCheckSummary {
            vertex_labels: scanned.vertex_inputs.len(),
            edge_labels: scanned.edge_inputs.len(),
            ..Default::default()
        };

        for vertex_input in &scanned.vertex_inputs {
            let Some(vertex_map) = snapshot.base().vertex_map(&vertex_input.label) else {
                summary.mismatches.push(format!(
                    "[vertex:{}] label missing from runtime snapshot",
                    vertex_input.label
                ));
                continue;
            };
            let expected_count = vertex_input.keys_and_rowids.len() as u64;
            let actual_count = snapshot
                .statistics()
                .vertex_count(&vertex_input.label)
                .unwrap_or(vertex_map.num_vertices() as u64);
            if expected_count != actual_count {
                summary.mismatches.push(format!(
                    "[vertex:{}] count mismatch: base={} graph={}",
                    vertex_input.label, expected_count, actual_count
                ));
            }

            for idx in sample_indices(vertex_input.keys_and_rowids.len(), cli.sample) {
                let (key, expected_rowid) = &vertex_input.keys_and_rowids[idx];
                summary.vertices_checked += 1;
                let Some(local_id) = vertex_map.key_to_local(key) else {
                    summary.mismatches.push(format!(
                        "[vertex:{}] missing key in graph: {}",
                        vertex_input.label,
                        format_vertex_key(key)
                    ));
                    continue;
                };
                let actual_rowid = vertex_map.local_to_rowid(local_id);
                if actual_rowid != *expected_rowid {
                    summary.mismatches.push(format!(
                        "[vertex:{}] rowid mismatch for key {}: base={} graph={}",
                        vertex_input.label,
                        format_vertex_key(key),
                        expected_rowid,
                        actual_rowid
                    ));
                }
            }
        }

        for edge_input in &scanned.edge_inputs {
            let Some(src_map) = snapshot.base().vertex_map(&edge_input.source_vertex_label) else {
                summary.mismatches.push(format!(
                    "[edge:{}] source label missing from runtime snapshot: {}",
                    edge_input.label, edge_input.source_vertex_label
                ));
                continue;
            };
            let Some(dst_map) = snapshot
                .base()
                .vertex_map(&edge_input.destination_vertex_label)
            else {
                summary.mismatches.push(format!(
                    "[edge:{}] destination label missing from runtime snapshot: {}",
                    edge_input.label, edge_input.destination_vertex_label
                ));
                continue;
            };

            let expected_count = edge_input.edges.len() as u64;
            let actual_count = snapshot
                .statistics()
                .edge_count(&edge_input.label)
                .unwrap_or(0);
            if expected_count != actual_count {
                summary.mismatches.push(format!(
                    "[edge:{}] count mismatch: base={} graph={}",
                    edge_input.label, expected_count, actual_count
                ));
            }

            let mut scratch = Vec::new();
            for idx in sample_indices(edge_input.edges.len(), cli.sample) {
                let (src_key, dst_key, edge_rowid) = &edge_input.edges[idx];
                summary.edges_checked += 1;
                let Some(src_local) = src_map.key_to_local(src_key) else {
                    summary.mismatches.push(format!(
                        "[edge:{}] source key missing in graph: {}",
                        edge_input.label,
                        format_vertex_key(src_key)
                    ));
                    continue;
                };
                let Some(dst_local) = dst_map.key_to_local(dst_key) else {
                    summary.mismatches.push(format!(
                        "[edge:{}] destination key missing in graph: {}",
                        edge_input.label,
                        format_vertex_key(dst_key)
                    ));
                    continue;
                };
                let Some(view) =
                    snapshot.neighbors_forward(&edge_input.label, src_local, &mut scratch)
                else {
                    summary.mismatches.push(format!(
                        "[edge:{}] missing forward adjacency view for source {}",
                        edge_input.label,
                        format_vertex_key(src_key)
                    ));
                    continue;
                };
                if !neighbor_contains(view, dst_local, *edge_rowid) {
                    summary.mismatches.push(format!(
                        "[edge:{}] missing sampled edge {} -> {} (rowid={})",
                        edge_input.label,
                        format_vertex_key(src_key),
                        format_vertex_key(dst_key),
                        edge_rowid
                    ));
                }
            }
        }

        println!(
            "graph={} state={:?} delta_size={} vertex_labels={} edge_labels={} vertices_checked={} edges_checked={} mismatches={}",
            graph.info.graph_name,
            snapshot.manifest().state(),
            snapshot.delta_size(),
            summary.vertex_labels,
            summary.edge_labels,
            summary.vertices_checked,
            summary.edges_checked,
            summary.mismatches.len()
        );
        for mismatch in &summary.mismatches {
            println!("  mismatch: {}", mismatch);
        }

        if !summary.mismatches.is_empty() {
            failed = true;
        }
    }

    if failed {
        Err("graph consistency check failed".to_string())
    } else {
        Ok(())
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{}", err);
            ExitCode::FAILURE
        }
    }
}
