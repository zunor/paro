//! Cross-process routine costing and bounded worker-pool requirements.

use std::collections::BTreeMap;

use paro_common::error::{self as paro_error, Result};

use super::cost::{CompactRange, ResourceDimension, ScoreSummary, SearchCost};
use super::ids::{
    ExternalWorkerPoolClassId, ExternalWorkerRequirementSetId, Fingerprint, ProgressSummaryId,
    RoutineCostProfileId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExternalWorkerRequirement {
    pub pool_class: ExternalWorkerPoolClassId,
    pub max_concurrent_calls: u16,
}

#[derive(Debug, Default)]
pub struct ExternalWorkerRequirementArena {
    sets: Vec<Box<[ExternalWorkerRequirement]>>,
    index: BTreeMap<Box<[ExternalWorkerRequirement]>, ExternalWorkerRequirementSetId>,
    max_requirements_per_set: u8,
}

impl ExternalWorkerRequirementArena {
    pub fn new(max_requirements_per_set: u8) -> Self {
        let mut arena = Self {
            max_requirements_per_set,
            ..Default::default()
        };
        arena.sets.push(Box::new([]));
        arena
            .index
            .insert(Box::new([]), ExternalWorkerRequirementSetId(0));
        arena
    }

    pub fn intern(
        &mut self,
        requirements: impl IntoIterator<Item = ExternalWorkerRequirement>,
    ) -> Result<ExternalWorkerRequirementSetId> {
        let mut by_pool = BTreeMap::new();
        for requirement in requirements {
            if requirement.max_concurrent_calls == 0 {
                continue;
            }
            by_pool
                .entry(requirement.pool_class)
                .and_modify(|slots: &mut u16| {
                    *slots = (*slots).max(requirement.max_concurrent_calls)
                })
                .or_insert(requirement.max_concurrent_calls);
        }
        if by_pool.len() > self.max_requirements_per_set as usize {
            return Err(paro_error::internal(
                "external worker requirement set exceeds its static bound",
            ));
        }
        let key: Box<_> = by_pool
            .into_iter()
            .map(
                |(pool_class, max_concurrent_calls)| ExternalWorkerRequirement {
                    pool_class,
                    max_concurrent_calls,
                },
            )
            .collect::<Vec<_>>()
            .into_boxed_slice();
        if let Some(id) = self.index.get(&key) {
            return Ok(*id);
        }
        let id = ExternalWorkerRequirementSetId::new(self.sets.len());
        self.sets.push(key.clone());
        self.index.insert(key, id);
        Ok(id)
    }

    pub fn union(
        &mut self,
        left: ExternalWorkerRequirementSetId,
        right: ExternalWorkerRequirementSetId,
    ) -> Result<ExternalWorkerRequirementSetId> {
        let left = self
            .sets
            .get(left.index())
            .ok_or_else(|| paro_error::internal("unknown external worker requirement set"))?
            .clone();
        let right = self
            .sets
            .get(right.index())
            .ok_or_else(|| paro_error::internal("unknown external worker requirement set"))?
            .clone();
        self.intern(left.iter().chain(right.iter()).copied())
    }

    pub fn get(&self, id: ExternalWorkerRequirementSetId) -> Option<&[ExternalWorkerRequirement]> {
        self.sets.get(id.index()).map(Box::as_ref)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExternalThermalState {
    ConservativeCold,
    WarmPool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ExternalCallWork {
    pub input_rows: CompactRange,
    pub input_bytes: CompactRange,
    pub output_bytes: CompactRange,
}

#[derive(Debug, Clone)]
pub struct RoutineCostProfile {
    pub id: RoutineCostProfileId,
    pub artifact: Fingerprint,
    pub runtime_abi: Fingerprint,
    pub pool_class: ExternalWorkerPoolClassId,
    pub batch_rows: u32,
    pub cold_start: CompactRange,
    pub warm_start: CompactRange,
    pub batch_dispatch: CompactRange,
    pub input_byte_cost: CompactRange,
    pub output_byte_cost: CompactRange,
    pub row_work: CompactRange,
    pub queue_delay_per_batch: CompactRange,
    pub max_concurrent_calls: u16,
    pub progress: ProgressSummaryId,
}

impl RoutineCostProfile {
    pub fn cost(
        &self,
        work: ExternalCallWork,
        thermal_state: ExternalThermalState,
        workers: &mut ExternalWorkerRequirementArena,
    ) -> Result<SearchCost> {
        self.validate()?;
        let batches = CompactRange::new(
            (work.input_rows.lower / self.batch_rows as f64).ceil(),
            (work.input_rows.expected / self.batch_rows as f64).ceil(),
            (work.input_rows.upper / self.batch_rows as f64).ceil(),
        )?;
        let startup = match thermal_state {
            ExternalThermalState::ConservativeCold => self.cold_start,
            ExternalThermalState::WarmPool => self.warm_start,
        };
        let dispatch = multiply(batches, self.batch_dispatch)?;
        let input_serialization = multiply(work.input_bytes, self.input_byte_cost)?;
        let output_serialization = multiply(work.output_bytes, self.output_byte_cost)?;
        let routine = multiply(work.input_rows, self.row_work)?;
        let queue = multiply(batches, self.queue_delay_per_batch)?;
        let latency = startup
            .checked_add(dispatch)?
            .checked_add(input_serialization)?
            .checked_add(output_serialization)?
            .checked_add(routine)?
            .checked_add(queue)?;
        let requirement = ExternalWorkerRequirement {
            pool_class: self.pool_class,
            max_concurrent_calls: self.max_concurrent_calls.max(1),
        };
        let worker_set = workers.intern([requirement])?;
        let expected_bytes = work.input_bytes.expected + work.output_bytes.expected;
        let risk_bytes = work.input_bytes.upper + work.output_bytes.upper;
        let mut result = SearchCost {
            score: ScoreSummary {
                range: latency,
                risk_adjusted: latency.expected + (latency.upper - latency.expected) * 0.5,
            },
            critical_path: latency,
            external_workers: worker_set,
            external_worker_slots_upper: requirement.max_concurrent_calls,
            progress: self.progress,
            ..SearchCost::ZERO
        };
        result.resources_expected[ResourceDimension::Network as usize] = expected_bytes;
        result.resources_risk_upper[ResourceDimension::Network as usize] = risk_bytes;
        result.resources_expected[ResourceDimension::Cpu as usize] =
            routine.expected + input_serialization.expected + output_serialization.expected;
        result.resources_risk_upper[ResourceDimension::Cpu as usize] =
            routine.upper + input_serialization.upper + output_serialization.upper;
        result.validate()?;
        Ok(result)
    }

    fn validate(&self) -> Result<()> {
        if self.batch_rows == 0 || self.max_concurrent_calls == 0 {
            return Err(paro_error::internal(
                "routine profile has zero batch size or worker concurrency",
            ));
        }
        if self.artifact == Fingerprint::default() || self.runtime_abi == Fingerprint::default() {
            return Err(paro_error::internal(
                "routine cost profile is not bound to artifact and runtime ABI revisions",
            ));
        }
        Ok(())
    }
}

fn multiply(left: CompactRange, right: CompactRange) -> Result<CompactRange> {
    CompactRange::new(
        left.lower * right.lower,
        left.expected * right.expected,
        left.upper * right.upper,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile(batch_rows: u32) -> RoutineCostProfile {
        RoutineCostProfile {
            id: RoutineCostProfileId(1),
            artifact: Fingerprint(1),
            runtime_abi: Fingerprint(2),
            pool_class: ExternalWorkerPoolClassId(3),
            batch_rows,
            cold_start: CompactRange::point(100.0).unwrap(),
            warm_start: CompactRange::point(1.0).unwrap(),
            batch_dispatch: CompactRange::point(10.0).unwrap(),
            input_byte_cost: CompactRange::point(0.1).unwrap(),
            output_byte_cost: CompactRange::point(0.1).unwrap(),
            row_work: CompactRange::point(1.0).unwrap(),
            queue_delay_per_batch: CompactRange::point(5.0).unwrap(),
            max_concurrent_calls: 2,
            progress: ProgressSummaryId(1),
        }
    }

    #[test]
    fn batching_changes_dispatch_queue_and_critical_path() {
        let work = ExternalCallWork {
            input_rows: CompactRange::point(100.0).unwrap(),
            input_bytes: CompactRange::point(1_000.0).unwrap(),
            output_bytes: CompactRange::point(500.0).unwrap(),
        };
        let mut workers = ExternalWorkerRequirementArena::new(4);
        let small = profile(10)
            .cost(work, ExternalThermalState::WarmPool, &mut workers)
            .unwrap();
        let large = profile(100)
            .cost(work, ExternalThermalState::WarmPool, &mut workers)
            .unwrap();
        assert!(large.critical_path.expected < small.critical_path.expected);
        assert_eq!(large.external_worker_slots_upper, 2);
    }

    #[test]
    fn worker_union_uses_max_slots_per_pool_not_sum() {
        let mut arena = ExternalWorkerRequirementArena::new(4);
        let a = arena
            .intern([ExternalWorkerRequirement {
                pool_class: ExternalWorkerPoolClassId(1),
                max_concurrent_calls: 2,
            }])
            .unwrap();
        let b = arena
            .intern([ExternalWorkerRequirement {
                pool_class: ExternalWorkerPoolClassId(1),
                max_concurrent_calls: 4,
            }])
            .unwrap();
        let union = arena.union(a, b).unwrap();
        assert_eq!(arena.get(union).unwrap()[0].max_concurrent_calls, 4);
    }
}
