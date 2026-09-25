// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use paro_common::config::format_setting_value;
use paro_common::runtime_value::Value;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::Duration;

/// Planning strategy, independent of semantic safety and search budgets.
/// No strategy promises exhaustive search when an isolation limit is hit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OptimizerSearchPolicy {
    /// Canonical plan, bounded region decisions and direct physical selection.
    #[default]
    Pipeline,
    /// Ordered relational stages followed by costing a closed candidate catalog.
    Regional,
    QualityCoverage,
    BudgetedSearch,
}

impl OptimizerSearchPolicy {
    pub fn parse(value: &str) -> paro_common::error::Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "pipeline" => Ok(Self::Pipeline),
            "regional" => Ok(Self::Regional),
            "quality" => Ok(Self::QualityCoverage),
            "budgeted" => Ok(Self::BudgetedSearch),
            _ => Err(paro_common::error::invalid_input(
                "optimizer_search_policy expects pipeline, regional, quality or budgeted",
            )),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pipeline => "pipeline",
            Self::Regional => "regional",
            Self::QualityCoverage => "quality",
            Self::BudgetedSearch => "budgeted",
        }
    }
}

/// Aggregate search domain of the staged planner, not a query-specific hint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OptimizerAggregateStrategy {
    #[default]
    Joint,
    SingleStage,
}

impl OptimizerAggregateStrategy {
    pub fn parse(value: &str) -> paro_common::error::Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "joint" => Ok(Self::Joint),
            "single_stage" => Ok(Self::SingleStage),
            _ => Err(paro_common::error::invalid_input(
                "optimizer_aggregate_strategy expects joint or single_stage",
            )),
        }
    }
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Joint => "joint",
            Self::SingleStage => "single_stage",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct EffectiveSettings {
    raw: HashMap<String, Value>,
}

impl EffectiveSettings {
    pub fn new(raw: HashMap<String, Value>) -> Self {
        Self { raw }
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.raw.get(&key.to_ascii_lowercase())
    }

    pub fn raw(&self) -> &HashMap<String, Value> {
        &self.raw
    }

    pub fn render(&self, key: &str) -> Option<String> {
        self.get(key)
            .map(|value| format_setting_value(&key.to_ascii_lowercase(), value))
    }

    pub fn optimizer_verify(&self) -> bool {
        matches!(self.get("optimizer_verify"), Some(Value::Boolean(true)))
    }

    pub fn disabled_optimizer_rules(&self) -> &str {
        match self.get("disabled_optimizer_rules") {
            Some(Value::Varchar(value)) => value,
            _ => "",
        }
    }

    pub fn optimizer_search_policy(&self) -> paro_common::error::Result<OptimizerSearchPolicy> {
        match self.get("optimizer_search_policy") {
            Some(Value::Varchar(value)) => OptimizerSearchPolicy::parse(value),
            None => Ok(OptimizerSearchPolicy::default()),
            _ => Err(paro_common::error::invalid_input(
                "invalid optimizer_search_policy type",
            )),
        }
    }

    pub fn force_external(&self) -> bool {
        matches!(self.get("force_external"), Some(Value::Boolean(true)))
    }

    pub fn optimizer_aggregate_strategy(
        &self,
    ) -> paro_common::error::Result<OptimizerAggregateStrategy> {
        match self.get("optimizer_aggregate_strategy") {
            Some(Value::Varchar(value)) => OptimizerAggregateStrategy::parse(value),
            None => Ok(OptimizerAggregateStrategy::default()),
            _ => Err(paro_common::error::invalid_input(
                "invalid optimizer_aggregate_strategy type",
            )),
        }
    }

    pub fn rowset_scan_pushdown(&self) -> bool {
        !matches!(
            self.get("rowset_scan_pushdown"),
            Some(Value::Boolean(false))
        )
    }

    pub fn vector_search_objective(&self) -> &str {
        match self.get("vector_search_objective") {
            Some(Value::Varchar(value)) => value,
            _ => "exact",
        }
    }

    pub fn parallel_scheduler(&self) -> bool {
        matches!(self.get("parallel_scheduler"), Some(Value::Boolean(true)))
    }

    pub fn threads(&self) -> Option<usize> {
        value_to_usize(self.get("threads"))
    }

    pub fn memory_limit(&self) -> Option<usize> {
        value_to_usize(self.get("memory_limit"))
    }

    pub fn temp_directory(&self) -> Option<String> {
        match self.get("temp_directory") {
            Some(Value::Varchar(value)) => Some(value.clone()),
            _ => None,
        }
    }

    pub fn max_temp_directory_size(&self) -> Option<Option<usize>> {
        match self.get("max_temp_directory_size") {
            Some(Value::Varchar(value)) if value.eq_ignore_ascii_case("unlimited") => Some(None),
            Some(value) => value_to_usize(Some(value)).map(Some),
            None => None,
        }
    }

    pub fn statement_timeout(&self) -> Option<Duration> {
        value_to_usize(self.get("statement_timeout"))
            .map(|millis| Duration::from_millis(millis as u64))
    }

    /// Fingerprint only settings that can change binding or physical planning.
    ///
    /// Runtime controls and diagnostics deliberately do not participate. In
    /// particular, enabling optimizer verification must validate the same
    /// immutable plan image rather than manufacturing a second cache entry.
    pub fn planning_fingerprint(&self) -> u64 {
        const PLAN_SETTINGS: &[&str] = &[
            "default_table_cardinality",
            "disabled_optimizer_rules",
            "optimizer_search_policy",
            "optimizer_aggregate_strategy",
            "force_external",
            "max_temp_directory_size",
            "memory_limit",
            "parallel_scheduler",
            "rowset_scan_pushdown",
            "temp_directory",
            "threads",
            "vector_search_objective",
        ];
        let mut hasher = DefaultHasher::new();
        for key in PLAN_SETTINGS {
            key.hash(&mut hasher);
            self.get(key).map(Value::to_string).hash(&mut hasher);
        }
        hasher.finish()
    }
}

fn value_to_usize(value: Option<&Value>) -> Option<usize> {
    match value {
        Some(Value::Integer(value)) if *value > 0 => Some(*value as usize),
        Some(Value::BigInt(value)) if *value > 0 => Some(*value as usize),
        Some(Value::Varchar(value)) => value.parse::<usize>().ok().filter(|value| *value > 0),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregate_search_domain_is_validated_and_part_of_cache_identity() {
        let settings = |value: &str| {
            EffectiveSettings::new(HashMap::from([(
                "optimizer_aggregate_strategy".into(),
                Value::Varchar(value.into()),
            )]))
        };
        assert_eq!(
            settings("single_stage")
                .optimizer_aggregate_strategy()
                .unwrap(),
            OptimizerAggregateStrategy::SingleStage
        );
        assert!(settings("force_partial")
            .optimizer_aggregate_strategy()
            .is_err());
        assert_ne!(
            settings("joint").planning_fingerprint(),
            settings("single_stage").planning_fingerprint()
        );
    }

    #[test]
    fn search_policy_is_validated_and_separates_plan_cache_identity() {
        assert_eq!(
            OptimizerSearchPolicy::default(),
            OptimizerSearchPolicy::Pipeline
        );
        let settings = |value: &str| {
            EffectiveSettings::new(HashMap::from([(
                "optimizer_search_policy".into(),
                Value::Varchar(value.into()),
            )]))
        };
        assert_eq!(
            settings("quality").optimizer_search_policy().unwrap(),
            OptimizerSearchPolicy::QualityCoverage
        );
        assert_eq!(
            settings("budgeted").optimizer_search_policy().unwrap(),
            OptimizerSearchPolicy::BudgetedSearch
        );
        assert_eq!(
            settings("regional").optimizer_search_policy().unwrap(),
            OptimizerSearchPolicy::Regional
        );
        assert_ne!(
            settings("regional").planning_fingerprint(),
            settings("budgeted").planning_fingerprint()
        );
        assert_ne!(
            settings("regional").planning_fingerprint(),
            settings("quality").planning_fingerprint()
        );
        assert!(settings("chain").optimizer_search_policy().is_err());
        assert_eq!(
            settings("pipeline").optimizer_search_policy().unwrap(),
            OptimizerSearchPolicy::Pipeline
        );
        assert_ne!(
            settings("pipeline").planning_fingerprint(),
            settings("regional").planning_fingerprint()
        );
        assert_ne!(
            settings("pipeline").planning_fingerprint(),
            settings("quality").planning_fingerprint()
        );
        assert_ne!(
            settings("quality").planning_fingerprint(),
            settings("budgeted").planning_fingerprint()
        );
    }

    #[test]
    fn planning_fingerprint_is_order_insensitive_but_value_sensitive() {
        let mut first = HashMap::new();
        first.insert("threads".to_string(), Value::Integer(4));
        first.insert("memory_limit".to_string(), Value::BigInt(1024));

        let mut second = HashMap::new();
        second.insert("memory_limit".to_string(), Value::BigInt(1024));
        second.insert("threads".to_string(), Value::Integer(4));

        let mut third = second.clone();
        third.insert("threads".to_string(), Value::Integer(8));

        assert_eq!(
            EffectiveSettings::new(first).planning_fingerprint(),
            EffectiveSettings::new(second).planning_fingerprint()
        );
        assert_ne!(
            EffectiveSettings::new(third).planning_fingerprint(),
            EffectiveSettings::new(HashMap::from([
                ("memory_limit".to_string(), Value::BigInt(1024)),
                ("threads".to_string(), Value::Integer(4)),
            ]))
            .planning_fingerprint()
        );
    }

    #[test]
    fn optimizer_verification_does_not_change_planning_identity() {
        let baseline =
            EffectiveSettings::new(HashMap::from([("threads".to_string(), Value::Integer(4))]));
        let verified = EffectiveSettings::new(HashMap::from([
            ("threads".to_string(), Value::Integer(4)),
            ("optimizer_verify".to_string(), Value::Boolean(true)),
        ]));

        assert_eq!(
            baseline.planning_fingerprint(),
            verified.planning_fingerprint()
        );
    }
}
