//! Graph statistics collected for property graph planning and introspection.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use super::{GraphBuildInput, GraphProjectionIndex};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PatternStepStatistic {
    pub source_label: String,
    pub edge_label: String,
    pub destination_label: String,
    pub count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DegreeHistogram {
    pub min: u32,
    pub max: u32,
    pub avg: f64,
    pub p50: u32,
    pub p90: u32,
    pub p99: u32,
}

impl DegreeHistogram {
    fn from_degrees(degrees: &[u32]) -> Self {
        if degrees.is_empty() {
            return Self {
                min: 0,
                max: 0,
                avg: 0.0,
                p50: 0,
                p90: 0,
                p99: 0,
            };
        }

        let mut sorted = degrees.to_vec();
        sorted.sort_unstable();
        let sum: u64 = sorted.iter().map(|&value| value as u64).sum();

        Self {
            min: sorted[0],
            max: *sorted.last().unwrap_or(&0),
            avg: sum as f64 / sorted.len() as f64,
            p50: percentile(&sorted, 0.50),
            p90: percentile(&sorted, 0.90),
            p99: percentile(&sorted, 0.99),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphStatistics {
    vertex_count: HashMap<String, u64>,
    edge_count: HashMap<String, u64>,
    pattern_step_count: Vec<PatternStepStatistic>,
    degree_histogram: HashMap<String, DegreeHistogram>,
    stats_version: u64,
}

pub trait GraphStatsProvider {
    fn vertex_count(&self, label: &str) -> Option<u64>;
    fn edge_count(&self, edge_label: &str) -> Option<u64>;
    fn pattern_step_count(&self, src_label: &str, edge_label: &str, dst_label: &str)
        -> Option<u64>;
    fn avg_degree(&self, label: &str) -> Option<f64>;
    fn degree_percentile(&self, label: &str, p: f64) -> Option<u32>;
}

impl Default for GraphStatistics {
    fn default() -> Self {
        Self {
            vertex_count: HashMap::new(),
            edge_count: HashMap::new(),
            pattern_step_count: Vec::new(),
            degree_histogram: HashMap::new(),
            stats_version: current_time_millis(),
        }
    }
}

impl GraphStatistics {
    pub fn with_vertex_count(mut self, label: &str, count: u64) -> Self {
        self.vertex_count.insert(label.to_string(), count);
        self
    }

    pub fn with_edge_count(mut self, label: &str, count: u64) -> Self {
        self.edge_count.insert(label.to_string(), count);
        self
    }

    pub fn with_pattern_step_count(
        mut self,
        src_label: &str,
        edge_label: &str,
        dst_label: &str,
        count: u64,
    ) -> Self {
        self.pattern_step_count.push(PatternStepStatistic {
            source_label: src_label.to_string(),
            edge_label: edge_label.to_string(),
            destination_label: dst_label.to_string(),
            count,
        });
        self
    }

    pub fn from_build_input(input: &GraphBuildInput) -> Self {
        let mut vertex_count = HashMap::with_capacity(input.vertex_tables.len());
        let mut edge_count = HashMap::with_capacity(input.edge_tables.len());
        let mut pattern_step_count = Vec::with_capacity(input.edge_tables.len());

        let mut vertex_keys = HashMap::with_capacity(input.vertex_tables.len());
        let mut vertex_degrees = HashMap::with_capacity(input.vertex_tables.len());

        for vertex in &input.vertex_tables {
            vertex_count.insert(vertex.label.clone(), vertex.keys_and_rowids.len() as u64);
            let keys = vertex
                .keys_and_rowids
                .iter()
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            let degree_map = vertex
                .keys_and_rowids
                .iter()
                .map(|(key, _)| (key.clone(), 0u32))
                .collect::<HashMap<_, _>>();
            vertex_keys.insert(vertex.label.clone(), keys);
            vertex_degrees.insert(vertex.label.clone(), degree_map);
        }

        for edge in &input.edge_tables {
            edge_count.insert(edge.label.clone(), edge.edges.len() as u64);
            pattern_step_count.push(PatternStepStatistic {
                source_label: edge.source_vertex_label.clone(),
                edge_label: edge.label.clone(),
                destination_label: edge.destination_vertex_label.clone(),
                count: edge.edges.len() as u64,
            });

            if let Some(source_degrees) = vertex_degrees.get_mut(&edge.source_vertex_label) {
                for (source_key, _, _) in &edge.edges {
                    if let Some(value) = source_degrees.get_mut(source_key) {
                        *value = value.saturating_add(1);
                    }
                }
            }

            if let Some(destination_degrees) =
                vertex_degrees.get_mut(&edge.destination_vertex_label)
            {
                for (_, destination_key, _) in &edge.edges {
                    if let Some(value) = destination_degrees.get_mut(destination_key) {
                        *value = value.saturating_add(1);
                    }
                }
            }
        }

        let degree_histogram = vertex_keys
            .into_iter()
            .map(|(label, keys)| {
                let degrees = keys
                    .iter()
                    .map(|key| {
                        vertex_degrees
                            .get(&label)
                            .and_then(|by_key| by_key.get(key))
                            .copied()
                            .unwrap_or(0)
                    })
                    .collect::<Vec<_>>();
                (label, DegreeHistogram::from_degrees(&degrees))
            })
            .collect();

        Self {
            vertex_count,
            edge_count,
            pattern_step_count,
            degree_histogram,
            stats_version: current_time_millis(),
        }
    }

    pub fn from_index(index: &GraphProjectionIndex) -> Self {
        let mut vertex_count = HashMap::new();
        let mut degree_vectors = HashMap::new();
        for label in index.vertex_labels() {
            let count = index
                .vertex_map(&label)
                .map(|map| map.num_vertices() as usize)
                .unwrap_or(0);
            vertex_count.insert(label.clone(), count as u64);
            degree_vectors.insert(label, vec![0u32; count]);
        }

        let mut edge_count = HashMap::new();
        let mut pattern_step_count = Vec::new();

        for edge_label in index.edge_labels() {
            let Some(csr) = index.forward_csr(&edge_label) else {
                continue;
            };
            let Some((source_label, destination_label)) = index.edge_endpoints(&edge_label) else {
                continue;
            };

            edge_count.insert(edge_label.clone(), csr.num_edges());
            pattern_step_count.push(PatternStepStatistic {
                source_label: source_label.to_string(),
                edge_label: edge_label.clone(),
                destination_label: destination_label.to_string(),
                count: csr.num_edges(),
            });

            if let Some(source_degrees) = degree_vectors.get_mut(source_label) {
                for source in 0..csr.num_vertices() {
                    let degree = csr.neighbors(source).len() as u32;
                    if let Some(value) = source_degrees.get_mut(source as usize) {
                        *value = value.saturating_add(degree);
                    }
                }
            }

            if let Some(destination_degrees) = degree_vectors.get_mut(destination_label) {
                for source in 0..csr.num_vertices() {
                    for &destination in csr.neighbors(source) {
                        if let Some(value) = destination_degrees.get_mut(destination as usize) {
                            *value = value.saturating_add(1);
                        }
                    }
                }
            }
        }

        let degree_histogram = degree_vectors
            .into_iter()
            .map(|(label, degrees)| (label, DegreeHistogram::from_degrees(&degrees)))
            .collect();

        Self {
            vertex_count,
            edge_count,
            pattern_step_count,
            degree_histogram,
            stats_version: current_time_millis(),
        }
    }

    pub fn stats_version(&self) -> u64 {
        self.stats_version
    }

    pub fn pattern_step_entries(&self) -> &[PatternStepStatistic] {
        &self.pattern_step_count
    }
}

impl GraphStatsProvider for GraphStatistics {
    fn vertex_count(&self, label: &str) -> Option<u64> {
        self.vertex_count.get(label).copied()
    }

    fn edge_count(&self, edge_label: &str) -> Option<u64> {
        self.edge_count.get(edge_label).copied()
    }

    fn pattern_step_count(
        &self,
        src_label: &str,
        edge_label: &str,
        dst_label: &str,
    ) -> Option<u64> {
        self.pattern_step_count
            .iter()
            .find(|entry| {
                entry.source_label == src_label
                    && entry.edge_label == edge_label
                    && entry.destination_label == dst_label
            })
            .map(|entry| entry.count)
    }

    fn avg_degree(&self, label: &str) -> Option<f64> {
        self.degree_histogram
            .get(label)
            .map(|histogram| histogram.avg)
    }

    fn degree_percentile(&self, label: &str, p: f64) -> Option<u32> {
        let histogram = self.degree_histogram.get(label)?;
        let percentile = (p * 100.0).round() as i32;
        match percentile {
            50 => Some(histogram.p50),
            90 => Some(histogram.p90),
            99 => Some(histogram.p99),
            _ => None,
        }
    }
}

fn percentile(sorted: &[u32], p: f64) -> u32 {
    if sorted.is_empty() {
        return 0;
    }

    let rank = ((sorted.len() - 1) as f64 * p).ceil() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

fn current_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::graph::{EdgeBuildInput, VertexBuildInput, VertexKey};

    fn social_graph_build_input() -> GraphBuildInput {
        GraphBuildInput {
            graph_name: "social_network".to_string(),
            vertex_tables: vec![
                VertexBuildInput {
                    label: "Person".to_string(),
                    keys_and_rowids: vec![
                        (VertexKey::Int64(1), 101),
                        (VertexKey::Int64(2), 102),
                        (VertexKey::Int64(3), 103),
                    ],
                },
                VertexBuildInput {
                    label: "Company".to_string(),
                    keys_and_rowids: vec![
                        (VertexKey::Int64(100), 201),
                        (VertexKey::Int64(200), 202),
                    ],
                },
            ],
            edge_tables: vec![
                EdgeBuildInput {
                    label: "Knows".to_string(),
                    source_vertex_label: "Person".to_string(),
                    destination_vertex_label: "Person".to_string(),
                    edges: vec![
                        (VertexKey::Int64(1), VertexKey::Int64(2), 9001),
                        (VertexKey::Int64(2), VertexKey::Int64(3), 9002),
                        (VertexKey::Int64(1), VertexKey::Int64(3), 9003),
                    ],
                },
                EdgeBuildInput {
                    label: "WorksAt".to_string(),
                    source_vertex_label: "Person".to_string(),
                    destination_vertex_label: "Company".to_string(),
                    edges: vec![
                        (VertexKey::Int64(1), VertexKey::Int64(100), 9101),
                        (VertexKey::Int64(2), VertexKey::Int64(200), 9102),
                    ],
                },
            ],
            build_backward_adjacency: true,
        }
    }

    #[test]
    fn graph_statistics_from_build_input_collects_counts_and_histograms() {
        let stats = GraphStatistics::from_build_input(&social_graph_build_input());

        assert_eq!(stats.vertex_count("Person"), Some(3));
        assert_eq!(stats.vertex_count("Company"), Some(2));
        assert_eq!(stats.edge_count("Knows"), Some(3));
        assert_eq!(stats.edge_count("WorksAt"), Some(2));
        assert_eq!(
            stats.pattern_step_count("Person", "Knows", "Person"),
            Some(3)
        );
        assert_eq!(
            stats.pattern_step_count("Person", "WorksAt", "Company"),
            Some(2)
        );
        assert_eq!(stats.avg_degree("Person"), Some(8.0 / 3.0));
        assert_eq!(stats.degree_percentile("Person", 0.90), Some(3));
        assert_eq!(stats.degree_percentile("Company", 0.50), Some(1));
        assert!(stats.stats_version() > 0);
    }
}
