// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Extensible operator-work registry folded into fixed machine resources.

use std::collections::BTreeMap;

use paro_common::error::{self as paro_error, Result};

use crate::physical::cost::{
    CompactRange, PhysicalCost, ResourceDimension, ScoreSummary, RESOURCE_DIMS,
};
use crate::physical::identity::{CalibrationRevisionId, OpClassId};

#[path = "calibration/generated.rs"]
mod generated;

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
/// One 32-byte block read or written by a tuple-processing implementation.
/// Keeping width in a separate class lets machine calibration vary memory
/// bandwidth independently of the operator's row-oriented CPU work.
pub const OP_TUPLE_BYTE_BLOCK: OpClassId = OpClassId(16);
/// One additional 32-byte block hashed beyond the integral-key baseline
/// already represented by the row-oriented hash operator classes.
pub const OP_HASH_KEY_BYTE_BLOCK: OpClassId = OpClassId(17);

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

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CalibratedOpCost {
    pub expected_resources_per_unit: [f64; RESOURCE_DIMS],
    pub risk_resources_per_unit: [f64; RESOURCE_DIMS],
    pub latency_per_unit: CompactRange,
}

/// Execution phase shape used to turn calibrated work into critical-path
/// span. Total resource work remains invariant across task counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParallelWorkProfile {
    /// The phase has no scheduler-visible parallel decomposition.
    Serial,
    /// Workers consume independent packets and only synchronize at the phase
    /// boundary.
    Pipeline,
}

#[derive(Debug, Clone)]
pub struct MachineCalibrationBundle {
    pub revision: CalibrationRevisionId,
    pub hardware_class: String,
    pub corpus_id: String,
    /// `bootstrap` is an explicitly provisional checked-in model; measured
    /// artifacts carry their immutable generation provenance here.
    pub provenance: String,
    coefficients: BTreeMap<OpClassId, CalibratedOpCost>,
    pub conservative_fallback: CalibratedOpCost,
    pub risk_weight: f64,
    expected_worker_efficiency: f64,
    risk_worker_efficiency: f64,
    coordination_latency_expected: f64,
    coordination_latency_upper: f64,
    pipeline_serial_fraction: f64,
    blocking_merge_serial_fraction: f64,
}

impl MachineCalibrationBundle {
    /// Versioned, generated calibration shipped with the engine. The source
    /// manifest records whether the coefficients are bootstrap or measured;
    /// production never silently presents provisional numbers as corpus-fit.
    pub fn builtin_production() -> Self {
        let mut bundle = Self::default();
        bundle.revision = CalibrationRevisionId(generated::REVISION);
        bundle.hardware_class = generated::HARDWARE_CLASS.to_string();
        bundle.corpus_id = generated::CORPUS_ID.to_string();
        bundle.provenance = generated::PROVENANCE.to_string();
        bundle.risk_weight = generated::RISK_WEIGHT;
        bundle.expected_worker_efficiency = generated::EXPECTED_WORKER_EFFICIENCY;
        bundle.risk_worker_efficiency = generated::RISK_WORKER_EFFICIENCY;
        bundle.coordination_latency_expected = generated::COORDINATION_LATENCY_EXPECTED;
        bundle.coordination_latency_upper = generated::COORDINATION_LATENCY_UPPER;
        bundle.pipeline_serial_fraction = generated::PIPELINE_SERIAL_FRACTION;
        bundle.blocking_merge_serial_fraction = generated::BLOCKING_MERGE_SERIAL_FRACTION;
        bundle.coefficients.clear();
        for coefficient in generated::COEFFICIENTS {
            bundle
                .set(
                    coefficient.class,
                    calibrated_dimension(
                        coefficient.dimension,
                        coefficient.expected,
                        coefficient.risk,
                        coefficient.latency_expected,
                        coefficient.latency_upper,
                    ),
                )
                .expect("built-in production calibration must be valid");
        }
        bundle
    }

    pub fn set(&mut self, class: OpClassId, cost: CalibratedOpCost) -> Result<()> {
        validate_calibrated_cost(cost)?;
        self.coefficients.insert(class, cost);
        Ok(())
    }

    pub fn fold(&self, work: &LocalOperatorWork) -> Result<PhysicalCost> {
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
        let result = PhysicalCost {
            score: ScoreSummary {
                range: score,
                risk_adjusted: expected_score + self.risk_weight * (risk_score - expected_score),
            },
            resources_expected,
            resources_risk_upper,
            work_latency: critical_path,
            critical_path,
            ..PhysicalCost::ZERO
        };
        result.validate()?;
        Ok(result)
    }

    /// Fold total work and derive span at one admitted physical operating
    /// point. This is Amdahl's law with calibrated imperfect worker scaling
    /// and an explicit coordination term. It deliberately does not divide
    /// resource work by DOP.
    pub fn fold_for_tasks(
        &self,
        work: &LocalOperatorWork,
        profile: ParallelWorkProfile,
        max_parallel_tasks: u16,
    ) -> Result<PhysicalCost> {
        let cost = self.fold(work)?;
        self.apply_parallelism(cost, profile, max_parallel_tasks)
    }

    pub fn apply_parallelism(
        &self,
        mut cost: PhysicalCost,
        profile: ParallelWorkProfile,
        max_parallel_tasks: u16,
    ) -> Result<PhysicalCost> {
        cost.max_parallel_tasks = max_parallel_tasks.max(1);
        let tasks = f64::from(max_parallel_tasks.max(1));
        if tasks == 1.0 || profile == ParallelWorkProfile::Serial {
            return Ok(cost);
        }
        let serial_fraction = match profile {
            ParallelWorkProfile::Serial => 1.0,
            ParallelWorkProfile::Pipeline => self.pipeline_serial_fraction,
        };
        let parallel_fraction = 1.0 - serial_fraction;
        let expected_workers = 1.0 + (tasks - 1.0) * self.expected_worker_efficiency;
        let risk_workers = 1.0 + (tasks - 1.0) * self.risk_worker_efficiency;
        let best_factor = serial_fraction + parallel_fraction / tasks;
        let expected_factor = serial_fraction + parallel_fraction / expected_workers;
        let risk_factor = serial_fraction + parallel_fraction / risk_workers;
        let extra_tasks = tasks - 1.0;
        cost.critical_path = CompactRange::new(
            cost.critical_path.lower * best_factor,
            cost.critical_path.expected * expected_factor
                + extra_tasks * self.coordination_latency_expected,
            cost.critical_path.upper * risk_factor + extra_tasks * self.coordination_latency_upper,
        )?;
        cost.validate()?;
        Ok(cost)
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
        coefficients.insert(
            OP_TUPLE_BYTE_BLOCK,
            calibrated_dimension(ResourceDimension::MemoryRead, 0.25, 1.0, 0.1, 0.5),
        );
        coefficients.insert(
            OP_HASH_KEY_BYTE_BLOCK,
            calibrated_dimension(ResourceDimension::Cpu, 0.45, 1.2, 0.45, 1.6),
        );
        Self {
            revision: CalibrationRevisionId(0),
            hardware_class: "conservative-fallback".into(),
            corpus_id: "builtin".into(),
            provenance: "conservative-fallback".into(),
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
            expected_worker_efficiency: 0.75,
            risk_worker_efficiency: 0.5,
            coordination_latency_expected: 256.0,
            coordination_latency_upper: 1024.0,
            pipeline_serial_fraction: 0.1,
            blocking_merge_serial_fraction: 0.25,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct BuiltinCoefficient {
    class: OpClassId,
    dimension: ResourceDimension,
    expected: f64,
    risk: f64,
    latency_expected: f64,
    latency_upper: f64,
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
    fn production_bundle_is_versioned_and_operator_specific() {
        let bundle = MachineCalibrationBundle::builtin_production();
        assert_eq!(bundle.revision, CalibrationRevisionId(3));
        assert_eq!(bundle.provenance, "bootstrap");
        for class in [
            OP_RUNTIME_FILTER_BUILD_ROW,
            OP_RUNTIME_FILTER_APPLY_ROW,
            OP_TUPLE_BYTE_BLOCK,
            OP_HASH_KEY_BYTE_BLOCK,
        ] {
            assert!(bundle.coefficients.contains_key(&class));
        }
        let mut build = LocalOperatorWork::default();
        build
            .add(OpClassId(1), CompactRange::point(10.0).unwrap())
            .unwrap();
        let mut probe = LocalOperatorWork::default();
        probe
            .add(OpClassId(2), CompactRange::point(10.0).unwrap())
            .unwrap();
        assert_ne!(
            bundle.fold(&build).unwrap().score.risk_adjusted,
            bundle.fold(&probe).unwrap().score.risk_adjusted
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

    #[test]
    fn task_count_changes_span_without_changing_total_work() {
        let mut work = LocalOperatorWork::default();
        work.add(OpClassId(999), CompactRange::point(100_000.0).unwrap())
            .unwrap();
        let calibration = MachineCalibrationBundle::default();
        let serial = calibration.fold(&work).unwrap();
        let parallel = calibration
            .fold_for_tasks(&work, ParallelWorkProfile::Pipeline, 4)
            .unwrap();
        assert_eq!(parallel.score, serial.score);
        assert_eq!(parallel.resources_expected, serial.resources_expected);
        assert!(parallel.critical_path.expected < serial.critical_path.expected);
    }

    #[test]
    fn coordination_prevents_free_parallelism_for_tiny_work() {
        let mut work = LocalOperatorWork::default();
        work.add(OpClassId(999), CompactRange::point(1.0).unwrap())
            .unwrap();
        let calibration = MachineCalibrationBundle::default();
        let serial = calibration.fold(&work).unwrap();
        let parallel = calibration
            .fold_for_tasks(&work, ParallelWorkProfile::Pipeline, 4)
            .unwrap();
        assert!(parallel.critical_path.expected > serial.critical_path.expected);
    }
}
