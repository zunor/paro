// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;

use paro_common::identity::GraphId;
use paro_common::runtime_value::Value;
use paro_context::StatementContext;
use paro_planner::binder::context::BindContext;
use paro_planner::logical::operator::ColumnBinding;
use paro_storage::index::graph::GraphStatistics;
use paro_storage::statistics::ColumnStatistics;

use crate::diagnostics::profile::OptimizerProfiler;
use crate::estimate::selectivity::SelectivityModel;

/// Immutable column statistics shared by candidate-local optimizer contexts.
pub type SharedColumnStatistics = Arc<HashMap<ColumnBinding, Arc<ColumnStatistics>>>;

pub trait GraphStatsLoader: Send + Sync {
    fn load(&self, graph_name: &str) -> Option<Arc<GraphStatistics>>;
}

pub struct GraphStatsCache {
    cache: HashMap<String, Arc<GraphStatistics>>,
    loader: Arc<dyn GraphStatsLoader>,
}

struct ContextGraphStatsLoader {
    context: Arc<StatementContext>,
}

pub(crate) struct EmptyGraphStatsLoader;

impl GraphStatsLoader for EmptyGraphStatsLoader {
    fn load(&self, _graph_name: &str) -> Option<Arc<GraphStatistics>> {
        None
    }
}

impl GraphStatsLoader for ContextGraphStatsLoader {
    fn load(&self, graph_name: &str) -> Option<Arc<GraphStatistics>> {
        self.context
            .graph_snapshot(&GraphId::new(
                self.context.current_database(),
                self.context.current_schema(),
                graph_name,
            ))
            .map(|snapshot| snapshot.statistics().clone())
    }
}

impl GraphStatsCache {
    pub fn with_loader(loader: Arc<dyn GraphStatsLoader>) -> Self {
        Self {
            cache: HashMap::new(),
            loader,
        }
    }

    pub fn get(&mut self, graph_name: &str) -> Option<Arc<GraphStatistics>> {
        if let Some(stats) = self.cache.get(graph_name) {
            return Some(stats.clone());
        }

        let stats = self.loader.load(graph_name)?;
        self.cache.insert(graph_name.to_string(), stats.clone());
        Some(stats)
    }
}

impl Default for GraphStatsCache {
    fn default() -> Self {
        Self::with_loader(Arc::new(EmptyGraphStatsLoader))
    }
}

pub struct OptimizationContext {
    pub session: Arc<StatementContext>,
    pub bind_context: BindContext,
    pub column_stats: SharedColumnStatistics,
    pub graph_stats: GraphStatsCache,
    pub cost_model: SelectivityModel,
    pub verify_enabled: bool,
    pub profiler: OptimizerProfiler,
}

impl OptimizationContext {
    pub fn new(session: Arc<StatementContext>, bind_context: BindContext) -> Self {
        let verify_enabled = should_verify(session.as_ref());
        Self {
            graph_stats: GraphStatsCache::with_loader(Arc::new(ContextGraphStatsLoader {
                context: session.clone(),
            })),
            session,
            bind_context,
            column_stats: Arc::new(HashMap::new()),
            cost_model: SelectivityModel::default(),
            verify_enabled,
            profiler: OptimizerProfiler::default(),
        }
    }

    /// Create an isolated estimation/search view for one logical candidate.
    ///
    /// Candidate generation must not communicate through `column_stats`: the
    /// set is keyed by plan-local bindings and adding an unrelated alternative
    /// must never change another alternative's join order or access path.
    pub(crate) fn fork_for_candidate(
        &self,
        column_stats: Arc<HashMap<ColumnBinding, Arc<ColumnStatistics>>>,
    ) -> Self {
        let mut candidate = Self::new(self.session.clone(), self.bind_context.clone());
        candidate.column_stats = column_stats;
        candidate.cost_model = self.cost_model.clone();
        candidate.verify_enabled = self.verify_enabled;
        candidate
    }

    /// Mutate a candidate's statistics through copy-on-write. Read-only
    /// physical alternatives share the immutable map; gathering detaches only
    /// when it actually publishes a new fact.
    pub(crate) fn column_stats_mut(
        &mut self,
    ) -> &mut HashMap<ColumnBinding, Arc<ColumnStatistics>> {
        Arc::make_mut(&mut self.column_stats)
    }
}

fn should_verify(ctx: &StatementContext) -> bool {
    if cfg!(any(test, debug_assertions)) {
        return true;
    }
    match ctx.get_setting("optimizer_verify") {
        Some(Value::Boolean(v)) => *v,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct MockGraphStatsLoader {
        calls: Arc<AtomicUsize>,
        stats: Arc<GraphStatistics>,
    }

    impl GraphStatsLoader for MockGraphStatsLoader {
        fn load(&self, graph_name: &str) -> Option<Arc<GraphStatistics>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            (graph_name == "g").then(|| self.stats.clone())
        }
    }

    #[test]
    fn graph_stats_cache_memoizes_provider_results() {
        let calls = Arc::new(AtomicUsize::new(0));
        let loader = Arc::new(MockGraphStatsLoader {
            calls: calls.clone(),
            stats: Arc::new(GraphStatistics::default()),
        });

        let mut cache = GraphStatsCache::with_loader(loader);
        assert!(cache.get("g").is_some());
        assert!(cache.get("g").is_some());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn graph_stats_cache_retries_when_loader_returns_none() {
        let calls = Arc::new(AtomicUsize::new(0));
        let loader = Arc::new(MockGraphStatsLoader {
            calls: calls.clone(),
            stats: Arc::new(GraphStatistics::default()),
        });

        let mut cache = GraphStatsCache::with_loader(loader);
        assert!(cache.get("missing").is_none());
        assert!(cache.get("missing").is_none());
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }
}
