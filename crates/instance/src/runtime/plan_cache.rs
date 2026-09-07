// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Bounded instance-wide cache for immutable compiled statement images.

use parking_lot::Mutex;
use paro_common::{logging::targets, types::LogicalType};
use paro_context::{CompileEnvironmentKey, StatementEnvironment};
use paro_execution::query_executor::compiled::CompiledStatement;
use paro_parser::ast::Statement;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};

const DEFAULT_PLAN_CACHE_CAPACITY: usize = 32;

#[derive(Debug)]
struct CachedPlan {
    statement: Statement,
    statement_format: Option<String>,
    parameter_types: Box<[LogicalType]>,
    environment: CompileEnvironmentKey,
    statement_environment: StatementEnvironment,
    plan: CompiledStatement,
}

impl CachedPlan {
    fn matches(
        &self,
        statement: &Statement,
        statement_format: Option<&str>,
        parameter_types: &[LogicalType],
        environment: &CompileEnvironmentKey,
        statement_environment: &StatementEnvironment,
    ) -> bool {
        self.statement == *statement
            && self.statement_format.as_deref() == statement_format
            && self.parameter_types.as_ref() == parameter_types
            && self.environment == *environment
            && self.statement_environment == *statement_environment
    }
}

/// Query-local runtime state is never cached here. `CompiledStatement` owns an
/// immutable program image, while the key names every binding, authorization,
/// parameter, formatting, catalog, and plan-setting input shared across sessions.
#[derive(Debug)]
pub struct InstancePlanCache {
    capacity: usize,
    entries: Mutex<VecDeque<CachedPlan>>,
    hits: AtomicU64,
    misses: AtomicU64,
    key_misses: AtomicU64,
    validation_misses: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstancePlanCacheMetrics {
    pub entries: usize,
    pub capacity: usize,
    pub hits: u64,
    pub misses: u64,
    pub key_misses: u64,
    pub validation_misses: u64,
}

impl Default for InstancePlanCache {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_PLAN_CACHE_CAPACITY)
    }
}

impl InstancePlanCache {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            entries: Mutex::new(VecDeque::with_capacity(capacity)),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            key_misses: AtomicU64::new(0),
            validation_misses: AtomicU64::new(0),
        }
    }

    pub fn get_validated(
        &self,
        statement: &Statement,
        statement_format: Option<&str>,
        parameter_types: &[LogicalType],
        environment: &CompileEnvironmentKey,
        statement_environment: &StatementEnvironment,
        validate: impl FnOnce(&CompiledStatement) -> bool,
    ) -> Option<CompiledStatement> {
        let mut entries = self.entries.lock();
        let Some(position) = entries.iter().position(|entry| {
            entry.matches(
                statement,
                statement_format,
                parameter_types,
                environment,
                statement_environment,
            )
        }) else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            self.key_misses.fetch_add(1, Ordering::Relaxed);
            let closest = entries.iter().find(|entry| {
                entry.statement == *statement
                    && entry.statement_format.as_deref() == statement_format
                    && entry.parameter_types.as_ref() == parameter_types
                    && entry.statement_environment == *statement_environment
            });
            tracing::debug!(
                target: targets::QUERY,
                cached_entries = entries.len(),
                cached_environment = ?closest.map(|entry| &entry.environment),
                requested_environment = ?environment,
                "Instance plan cache key mismatch"
            );
            return None;
        };
        let entry = entries
            .remove(position)
            .expect("located plan-cache entry must remain present while locked");
        let plan = entry.plan.clone();
        if !validate(&plan) {
            self.misses.fetch_add(1, Ordering::Relaxed);
            self.validation_misses.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(
                target: targets::QUERY,
                "Instance plan cache dynamic dependency validation failed"
            );
            return None;
        }
        entries.push_back(entry);
        self.hits.fetch_add(1, Ordering::Relaxed);
        Some(plan)
    }

    pub fn publish(
        &self,
        statement: Statement,
        statement_format: Option<String>,
        parameter_types: Vec<LogicalType>,
        environment: CompileEnvironmentKey,
        statement_environment: StatementEnvironment,
        plan: CompiledStatement,
    ) {
        if self.capacity == 0 {
            return;
        }
        let mut entries = self.entries.lock();
        if let Some(position) = entries.iter().position(|entry| {
            entry.matches(
                &statement,
                statement_format.as_deref(),
                &parameter_types,
                &environment,
                &statement_environment,
            )
        }) {
            entries.remove(position);
        }
        while entries.len() >= self.capacity {
            entries.pop_front();
        }
        entries.push_back(CachedPlan {
            statement,
            statement_format,
            parameter_types: parameter_types.into_boxed_slice(),
            environment,
            statement_environment,
            plan,
        });
    }

    pub fn metrics(&self) -> InstancePlanCacheMetrics {
        InstancePlanCacheMetrics {
            entries: self.entries.lock().len(),
            capacity: self.capacity,
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            key_misses: self.key_misses.load(Ordering::Relaxed),
            validation_misses: self.validation_misses.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::InstancePlanCache;

    #[test]
    fn zero_capacity_is_an_explicit_disabled_cache() {
        let cache = InstancePlanCache::with_capacity(0);
        let metrics = cache.metrics();
        assert_eq!(metrics.capacity, 0);
        assert_eq!(metrics.entries, 0);
        assert_eq!(metrics.key_misses, 0);
        assert_eq!(metrics.validation_misses, 0);
    }
}
