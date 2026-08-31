// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Extensible operator-work registry folded into fixed machine resources.

use std::collections::BTreeMap;

use paro_common::error::{self as paro_error, Result};

use super::cost::{CompactRange, ResourceDimension, ScoreSummary, SearchCost, RESOURCE_DIMS};
use super::ids::{CalibrationRevisionId, OpClassId};

pub const MAX_LOCAL_OP_CLASSES: usize = 16;

// Stable built-in classes used by the property-enforcement cost model. They
// live in the calibration namespace so a published machine bundle can replace
// the conservative coefficients without changing optimizer code.
pub const OP_ENFORCER_STREAM_ROW: OpClassId = OpClassId(20_001);
pub const OP_ENFORCER_SORT_COMPARE: OpClassId = OpClassId(20_002);
pub const OP_ENFORCER_RANDOM_FETCH: OpClassId = OpClassId(20_003);
pub const OP_ENFORCER_SPILL_PAGE: OpClassId = OpClassId(20_004);
pub const OP_RUNTIME_FILTER_BUILD_ROW: OpClassId = OpClassId(11);
pub const OP_RUNTIME_FILTER_APPLY_ROW: OpClassId = OpClassId(12);

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WorkUnit {
    pub class: OpClassId,
    pub units: CompactRange,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LocalOperatorWork {
    units: [Option<WorkUnit>; MAX_LOCAL_OP_CLASSES],
    len: u8,
}

impl Default for LocalOperatorWork {
    fn default() -> Self {
        Self {
            units: [None; MAX_LOCAL_OP_CLASSES],
            len: 0,
        }
    }
}

impl LocalOperatorWork {
    pub fn add(&mut self, class: OpClassId, units: CompactRange) -> Result<()> {
        if let Some(existing) = self.units[..self.len as usize]
            .iter_mut()
            .flatten()
            .find(|entry| entry.class == class)
        {
            existing.units = existing.units.checked_add(units)?;
            return Ok(());
        }
        if self.len as usize == MAX_LOCAL_OP_CLASSES {
            return Err(paro_error::internal(
                "local work exceeds the static OpClass bound; register a composite class",
            ));
        }
        self.units[self.len as usize] = Some(WorkUnit { class, units });
        self.len += 1;
        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = WorkUnit> + '_ {
        self.units[..self.len as usize].iter().flatten().copied()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpClassSchema {
    pub id: OpClassId,
    pub stable_name: String,
}

#[derive(Debug, Default)]
pub struct OpClassRegistry {
    classes: BTreeMap<OpClassId, OpClassSchema>,
}

impl OpClassRegistry {
    pub fn register(&mut self, schema: OpClassSchema) -> Result<()> {
        if schema.stable_name.is_empty() {
            return Err(paro_error::internal("OpClass stable name is empty"));
        }
        if self.classes.insert(schema.id, schema).is_some() {
            return Err(paro_error::internal("duplicate OpClassId"));
        }
        Ok(())
    }

    pub fn contains(&self, id: OpClassId) -> bool {
        self.classes.contains_key(&id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CalibratedOpCost {
    pub expected_resources_per_unit: [f64; RESOURCE_DIMS],
    pub risk_resources_per_unit: [f64; RESOURCE_DIMS],
    pub latency_per_unit: CompactRange,
}

#[derive(Debug, Clone)]
pub struct MachineCalibrationBundle {
    pub revision: CalibrationRevisionId,
    pub hardware_class: String,
    pub corpus_id: String,
    coefficients: BTreeMap<OpClassId, CalibratedOpCost>,
    pub conservative_fallback: CalibratedOpCost,
    pub risk_weight: f64,
}

impl MachineCalibrationBundle {
    pub fn set(&mut self, class: OpClassId, cost: CalibratedOpCost) -> Result<()> {
        validate_calibrated_cost(cost)?;
        self.coefficients.insert(class, cost);
        Ok(())
    }

    pub fn fold(&self, work: &LocalOperatorWork) -> Result<SearchCost> {
        let mut resources_expected = [0.0; RESOURCE_DIMS];
        let mut resources_risk_upper = [0.0; RESOURCE_DIMS];
        let mut critical_path = CompactRange::ZERO;
        for unit in work.iter() {
            let coefficient = self
                .coefficients
                .get(&unit.class)
                .copied()
                .unwrap_or(self.conservative_fallback);
            for dimension in 0..RESOURCE_DIMS {
                resources_expected[dimension] +=
                    unit.units.expected * coefficient.expected_resources_per_unit[dimension];
                resources_risk_upper[dimension] +=
                    unit.units.upper * coefficient.risk_resources_per_unit[dimension];
            }
            critical_path = critical_path.checked_add(CompactRange::new(
                unit.units.lower * coefficient.latency_per_unit.lower,
                unit.units.expected * coefficient.latency_per_unit.expected,
                unit.units.upper * coefficient.latency_per_unit.upper,
            )?)?;
        }
        let expected_score: f64 = resources_expected.iter().sum();
        let risk_score: f64 = resources_risk_upper.iter().sum();
        let score = CompactRange::new(expected_score.min(risk_score), expected_score, risk_score)?;
        let result = SearchCost {
            score: ScoreSummary {
                range: score,
                risk_adjusted: expected_score + self.risk_weight * (risk_score - expected_score),
            },
            resources_expected,
            resources_risk_upper,
            critical_path,
            ..SearchCost::ZERO
        };
        result.validate()?;
        Ok(result)
    }
}

fn validate_calibrated_cost(cost: CalibratedOpCost) -> Result<()> {
    let values = cost
        .expected_resources_per_unit
        .into_iter()
        .chain(cost.risk_resources_per_unit)
        .chain([
            cost.latency_per_unit.lower,
            cost.latency_per_unit.expected,
            cost.latency_per_unit.upper,
        ]);
    if values
        .into_iter()
        .any(|value| !value.is_finite() || value < 0.0)
    {
        return Err(paro_error::internal(
            "calibration coefficient is negative or non-finite",
        ));
    }
    Ok(())
}

impl Default for MachineCalibrationBundle {
    fn default() -> Self {
        let mut expected = [0.0; RESOURCE_DIMS];
        let mut risk = [0.0; RESOURCE_DIMS];
        expected[ResourceDimension::Cpu as usize] = 1.0;
        risk[ResourceDimension::Cpu as usize] = 4.0;
        let mut coefficients = BTreeMap::new();
        coefficients.insert(
            OP_ENFORCER_STREAM_ROW,
            calibrated_dimension(ResourceDimension::Cpu, 0.25, 1.0, 0.25, 1.0),
        );
        coefficients.insert(
            OP_ENFORCER_SORT_COMPARE,
            calibrated_dimension(ResourceDimension::Cpu, 1.0, 4.0, 1.0, 6.0),
        );
        coefficients.insert(
            OP_ENFORCER_RANDOM_FETCH,
            calibrated_dimension(ResourceDimension::RandomIo, 1.0, 6.0, 2.0, 12.0),
        );
        coefficients.insert(
            OP_ENFORCER_SPILL_PAGE,
            calibrated_dimension(ResourceDimension::SequentialIo, 2.0, 8.0, 2.0, 10.0),
        );
        coefficients.insert(
            OP_RUNTIME_FILTER_BUILD_ROW,
            calibrated_dimension(ResourceDimension::Cpu, 0.15, 0.6, 0.15, 0.8),
        );
        coefficients.insert(
            OP_RUNTIME_FILTER_APPLY_ROW,
            calibrated_dimension(ResourceDimension::Cpu, 0.1, 0.4, 0.1, 0.6),
        );
        Self {
            revision: CalibrationRevisionId(0),
            hardware_class: "conservative-fallback".into(),
            corpus_id: "builtin".into(),
            coefficients,
            conservative_fallback: CalibratedOpCost {
                expected_resources_per_unit: expected,
                risk_resources_per_unit: risk,
                latency_per_unit: CompactRange {
                    lower: 1.0,
                    expected: 2.0,
                    upper: 8.0,
                },
            },
            risk_weight: 0.5,
        }
    }
}

fn calibrated_dimension(
    dimension: ResourceDimension,
    expected: f64,
    risk: f64,
    latency_expected: f64,
    latency_upper: f64,
) -> CalibratedOpCost {
    let mut expected_resources_per_unit = [0.0; RESOURCE_DIMS];
    let mut risk_resources_per_unit = [0.0; RESOURCE_DIMS];
    expected_resources_per_unit[dimension as usize] = expected;
    risk_resources_per_unit[dimension as usize] = risk;
    CalibratedOpCost {
        expected_resources_per_unit,
        risk_resources_per_unit,
        latency_per_unit: CompactRange {
            lower: latency_expected * 0.5,
            expected: latency_expected,
            upper: latency_upper,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_class_uses_versioned_conservative_fallback() {
        let mut work = LocalOperatorWork::default();
        work.add(OpClassId(999), CompactRange::point(10.0).unwrap())
            .unwrap();
        let cost = MachineCalibrationBundle::default().fold(&work).unwrap();
        assert_eq!(
            cost.resources_expected[ResourceDimension::Cpu as usize],
            10.0
        );
        assert_eq!(
            cost.resources_risk_upper[ResourceDimension::Cpu as usize],
            40.0
        );
    }

    #[test]
    fn static_work_bound_is_enforced_without_heap_fallback() {
        let mut work = LocalOperatorWork::default();
        for id in 0..MAX_LOCAL_OP_CLASSES {
            work.add(OpClassId(id as u32), CompactRange::point(1.0).unwrap())
                .unwrap();
        }
        assert!(work
            .add(OpClassId(100), CompactRange::point(1.0).unwrap())
            .is_err());
    }
}
