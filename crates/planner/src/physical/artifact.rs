// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! One immutable physical plan and the resources needed to execute it.
//! Admission checks availability; it never ranks alternatives or replans.

use super::cost::MemoryCompletion;
use super::{Fingerprint, PhysicalPlan, PhysicalPlanVerifier, ResourceGrantClassId};
use paro_common::error::{self as error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SpillPolicy {
    Forbidden,
    Allowed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResourceGrantClass {
    pub id: ResourceGrantClassId,
    pub hard_memory_bytes: u64,
    pub spill_policy: SpillPolicy,
    pub max_parallel_tasks: u16,
}

/// A resource contract is not a lease. Execution acquires a lifetime-owned
/// lease before lowering or running the admitted plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionResourceContract {
    pub class: ResourceGrantClassId,
    pub minimum_memory_bytes: u64,
    pub working_set_memory_bytes: u64,
    pub memory_ceiling_bytes: u64,
    pub memory_completion: MemoryCompletion,
    pub max_parallel_tasks: u16,
    pub external_worker_slots: u16,
}

#[derive(Debug, Clone)]
pub struct CompiledPhysicalPlan {
    pub plan: PhysicalPlan,
    pub grant: ResourceGrantClass,
    pub physical_fingerprint: Fingerprint,
}

#[derive(Debug)]
pub struct AdmittedPlan {
    pub plan: PhysicalPlan,
    pub physical_fingerprint: Fingerprint,
    pub resources: ExecutionResourceContract,
}

impl CompiledPhysicalPlan {
    pub fn new(
        grant: ResourceGrantClass,
        plan: PhysicalPlan,
        physical_fingerprint: Fingerprint,
    ) -> Result<Self> {
        let artifact = Self {
            plan,
            grant,
            physical_fingerprint,
        };
        artifact.verify()?;
        Ok(artifact)
    }

    pub fn verify_result_types(&self, expected: &[paro_common::types::LogicalType]) -> Result<()> {
        // Internal write-completion rows are not a client result schema.
        if !expected.is_empty() && self.plan.node(self.plan.root).output.types.as_ref() != expected
        {
            return Err(error::internal(
                "physical root violates the compiled result presentation",
            ));
        }
        Ok(())
    }

    pub fn verify(&self) -> Result<()> {
        PhysicalPlanVerifier::verify(&self.plan)?;
        if self.plan.execution_resources.is_some() {
            return Err(error::internal(
                "compiled plan contains pre-bound execution resources",
            ));
        }
        if self.grant.max_parallel_tasks == 0 {
            return Err(error::internal(
                "compiled plan requires zero execution tasks",
            ));
        }
        for (_, properties) in self.plan.properties.iter() {
            if !properties
                .grant_contract
                .accepts(self.grant.id, self.grant.max_parallel_tasks)
            {
                return Err(error::internal(
                    "physical node disagrees with the compiled resource contract",
                ));
            }
        }
        let cost = self
            .plan
            .properties
            .get(self.plan.root)
            .ok_or_else(|| error::internal("compiled plan has no root properties"))?
            .cumulative_cost;
        cost.validate()?;
        if cost.peak_memory_upper > self.grant.hard_memory_bytes {
            return Err(error::internal("compiled plan exceeds its memory envelope"));
        }
        if self.grant.spill_policy == SpillPolicy::Forbidden {
            use super::{PhysicalNodeKind, SpillExecutionPolicy};
            if cost.spill_bytes_expected > 0
                || self.plan.nodes.iter().any(|node| match &node.kind {
                    PhysicalNodeKind::Aggregate(spec) => {
                        spec.spill_policy != SpillExecutionPolicy::InMemory
                    }
                    PhysicalNodeKind::HashJoin(spec) => {
                        spec.spill_policy != SpillExecutionPolicy::InMemory
                    }
                    PhysicalNodeKind::Sort(spec) => {
                        spec.spill_policy != SpillExecutionPolicy::InMemory
                    }
                    _ => false,
                })
            {
                return Err(error::internal(
                    "spill-capable plan has a no-spill resource contract",
                ));
            }
        }
        Ok(())
    }

    pub fn admit<F>(
        &self,
        available_memory_bytes: u64,
        available_parallel_tasks: u16,
        available_external_worker_slots: u16,
        dependency_available: F,
    ) -> Result<AdmittedPlan>
    where
        F: Fn(&PhysicalPlan) -> bool,
    {
        self.verify()?;
        let cost = self
            .plan
            .properties
            .get(self.plan.root)
            .expect("verified root")
            .cumulative_cost;
        if !dependency_available(&self.plan)
            || cost.minimum_memory_bytes > available_memory_bytes
            || (self.grant.hard_memory_bytes > available_memory_bytes
                && cost.preferred_memory_bytes() > available_memory_bytes)
            || self.grant.max_parallel_tasks > available_parallel_tasks
            || cost.external_worker_slots_upper > available_external_worker_slots
        {
            return Err(error::configuration_limit_exceeded(
                "compiled plan does not fit the available resources or dependencies",
            ));
        }
        Ok(AdmittedPlan {
            plan: self.plan.clone(),
            physical_fingerprint: self.physical_fingerprint,
            resources: ExecutionResourceContract {
                class: self.grant.id,
                minimum_memory_bytes: cost.minimum_memory_bytes,
                working_set_memory_bytes: cost.preferred_memory_bytes(),
                memory_ceiling_bytes: self.grant.hard_memory_bytes,
                memory_completion: cost.memory_completion,
                max_parallel_tasks: self.grant.max_parallel_tasks,
                external_worker_slots: cost.external_worker_slots_upper,
            },
        })
    }
}
