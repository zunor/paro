// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! The resource observation shared by compilation and its cache identity.

/// A declared operating point, independent of optimizer-specific identities.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompileGrant {
    pub index: usize,
    pub hard_memory_bytes: u64,
    pub max_parallel_tasks: u16,
}

/// Captured once when a statement is frozen. This is an observation, not a
/// reservation: admission still acquires and verifies execution resources.
/// Memory is the shared query-pool envelope after reserves/retained bytes,
/// not a concurrency-adjusted per-query fair share or an exact future grant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompileResources {
    pub available_memory_bytes: u64,
    pub available_parallel_tasks: u16,
}

impl CompileResources {
    pub fn capture(available_memory: usize, available_threads: usize) -> Self {
        Self {
            available_memory_bytes: u64::try_from(available_memory).unwrap_or(u64::MAX),
            available_parallel_tasks: available_threads.min(u16::MAX as usize) as u16,
        }
    }

    /// One operating point shared by the compiler and cache key. A zero
    /// configured memory limit means no session cap, not infinite availability.
    pub fn expected_grant(self, max_memory: usize, max_threads: usize) -> Option<CompileGrant> {
        let configured_memory = if max_memory == 0 {
            u64::MAX
        } else {
            max_memory as u64
        };
        let hard_memory_bytes = configured_memory.min(self.available_memory_bytes);
        let max_parallel_tasks = max_threads.clamp(1, u16::MAX as usize) as u16;
        let max_parallel_tasks = max_parallel_tasks.min(self.available_parallel_tasks);
        (hard_memory_bytes > 0 && max_parallel_tasks > 0).then_some(CompileGrant {
            index: 0,
            hard_memory_bytes,
            max_parallel_tasks,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn one_operating_point_is_bounded_by_settings_and_frozen_availability() {
        let full = CompileResources::capture(1024, 4);
        assert_eq!(
            full.expected_grant(1024, 4).unwrap(),
            CompileGrant {
                index: 0,
                hard_memory_bytes: 1024,
                max_parallel_tasks: 4
            }
        );
        assert_eq!(
            CompileResources::capture(512, 1)
                .expected_grant(1024, 4)
                .unwrap(),
            CompileGrant {
                index: 0,
                hard_memory_bytes: 512,
                max_parallel_tasks: 1
            }
        );
        assert_eq!(
            full.expected_grant(512, 2).unwrap(),
            CompileGrant {
                index: 0,
                hard_memory_bytes: 512,
                max_parallel_tasks: 2
            }
        );
        assert_eq!(full.expected_grant(0, 4).unwrap().hard_memory_bytes, 1024);
        assert!(CompileResources::capture(0, 4)
            .expected_grant(1024, 4)
            .is_none());
        assert!(CompileResources::capture(1024, 0)
            .expected_grant(1024, 4)
            .is_none());
    }
}
