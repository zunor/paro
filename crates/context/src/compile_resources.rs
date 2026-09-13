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

    pub fn expected_grant(
        self,
        max_memory: usize,
        max_threads: usize,
        max_classes: u8,
    ) -> Option<CompileGrant> {
        compile_grant_classes(max_memory, max_threads, max_classes)
            .into_iter()
            .filter(|class| {
                class.hard_memory_bytes <= self.available_memory_bytes
                    && class.max_parallel_tasks <= self.available_parallel_tasks
            })
            .max_by_key(|class| {
                (
                    class.max_parallel_tasks,
                    class.hard_memory_bytes,
                    class.index,
                )
            })
    }
}

/// Keep the existing bounded operating points identical across the cache and
/// optimizer. This does not change the configured number of grant classes.
pub fn compile_grant_classes(
    max_memory: usize,
    max_threads: usize,
    max_classes: u8,
) -> Vec<CompileGrant> {
    let full = u64::try_from(max_memory).unwrap_or(u64::MAX).max(1);
    let hard_limits = if max_memory == 0 {
        vec![u64::MAX]
    } else {
        match max_classes.clamp(1, 3) {
            1 => vec![full],
            2 => vec![(full / 2).max(1), full],
            _ => vec![(full / 4).max(1), (full / 2).max(1), full],
        }
    };
    let count = hard_limits.len();
    let threads = max_threads.clamp(1, u16::MAX as usize);
    let mut previous = None;
    hard_limits
        .into_iter()
        .filter(|hard| previous.replace(*hard) != Some(*hard))
        .enumerate()
        .map(|(index, hard_memory_bytes)| CompileGrant {
            index,
            hard_memory_bytes,
            max_parallel_tasks: match count {
                1 => threads,
                2 if index == 0 => 1,
                2 => threads,
                _ if index == 0 => 1,
                _ if index + 1 == count => threads,
                _ => threads.div_ceil(2),
            } as u16,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expected_grant_uses_both_frozen_availability_and_limits() {
        let full = CompileResources::capture(1024, 4);
        assert_eq!(full.expected_grant(1024, 4, 3).unwrap().index, 2);
        assert_eq!(
            CompileResources::capture(512, 4)
                .expected_grant(1024, 4, 3)
                .unwrap()
                .index,
            1
        );
        assert_eq!(
            CompileResources::capture(1024, 1)
                .expected_grant(1024, 4, 3)
                .unwrap()
                .index,
            0
        );
        assert!(CompileResources::capture(128, 4)
            .expected_grant(1024, 4, 3)
            .is_none());
        assert!(CompileResources::capture(1024, 0)
            .expected_grant(1024, 4, 3)
            .is_none());
        assert!(
            CompileResources::capture(1024, 4)
                .expected_grant(0, 4, 3)
                .is_none(),
            "an unbounded setting is not observed infinite availability"
        );
        assert_eq!(
            full.expected_grant(512, 2, 3).unwrap().hard_memory_bytes,
            512
        );
    }
}
