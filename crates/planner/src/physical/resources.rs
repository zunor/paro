// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Executable memory contracts shared by physical planning and admission.
//!
//! A spill capability is not itself a memory proof. Each implementation
//! declares the resident state required to make progress, task-local scratch
//! at its maximum admitted concurrency, and the revocable working-set target.

use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExecutionMemoryContract {
    pub fixed_non_revocable_bytes: u64,
    pub fixed_scratch_bytes: u64,
    pub per_task_scratch_bytes: u64,
    pub max_concurrent_tasks: u16,
    pub revocable_minimum_bytes: u64,
    pub revocable_target_bytes: u64,
    pub spill_buffer_minimum_bytes: u64,
}

impl ExecutionMemoryContract {
    pub fn minimum_memory_bytes(self) -> Result<u64> {
        self.fixed_non_revocable_bytes
            .checked_add(self.fixed_scratch_bytes)
            .and_then(|bytes| {
                bytes.checked_add(
                    self.per_task_scratch_bytes
                        .checked_mul(u64::from(self.max_concurrent_tasks))?,
                )
            })
            .and_then(|bytes| bytes.checked_add(self.revocable_minimum_bytes))
            .and_then(|bytes| bytes.checked_add(self.spill_buffer_minimum_bytes))
            .ok_or_else(|| paro_error::internal("execution memory floor overflow"))
    }

    pub fn preferred_memory_bytes(self) -> Result<u64> {
        self.minimum_memory_bytes()?
            .checked_add(self.revocable_target_bytes)
            .ok_or_else(|| paro_error::internal("execution memory target overflow"))
    }

    pub fn validate(self) -> Result<()> {
        if self.max_concurrent_tasks == 0 && self.per_task_scratch_bytes != 0 {
            return Err(paro_error::internal(
                "execution memory contract has task scratch without task concurrency",
            ));
        }
        let _ = self.preferred_memory_bytes()?;
        Ok(())
    }
}

/// Resident floor for one external blocking task: two vector/encoding blocks,
/// one row-store spill page, and fixed writer/state metadata.
pub const BLOCKING_FIXED_SCRATCH_BYTES: u64 = 64 * 1024;
pub const BLOCKING_PER_TASK_SCRATCH_BYTES: u64 =
    (paro_storage::buffer::DEFAULT_BLOCK_ALLOC_SIZE as u64) * 2;
pub const SPILL_BUFFER_MINIMUM_BYTES: u64 = paro_storage::buffer::DEFAULT_BLOCK_ALLOC_SIZE as u64;

/// Physical capability of a published join-side filter. Adaptive membership
/// may degrade to a min/max range under either domain or memory pressure; it
/// is never a hard survivor-cardinality proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeFilterCapability {
    Disabled,
    Range,
    AdaptiveExactMembership,
    AdaptivePerKeyMembership,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeFilterKeyRepresentation {
    Disabled,
    Range,
    ExactI32,
    ExactI64,
    ExactI128,
}

impl RuntimeFilterKeyRepresentation {
    pub fn is_exact(self) -> bool {
        matches!(self, Self::ExactI32 | Self::ExactI64 | Self::ExactI128)
    }

    pub fn value_width(self) -> usize {
        match self {
            Self::Disabled | Self::Range => 0,
            Self::ExactI32 => std::mem::size_of::<i32>(),
            Self::ExactI64 => std::mem::size_of::<i64>(),
            Self::ExactI128 => std::mem::size_of::<i128>(),
        }
    }
}

/// One immutable optimizer/executor contract for runtime-filter construction,
/// merge, freeze, and fallback. Memory is charged for all local builders, the
/// merged representation, and the conversion overlap before any allocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeFilterResourceContract {
    pub capability: RuntimeFilterCapability,
    pub keys: Box<[RuntimeFilterKeyRepresentation]>,
    pub max_local_builders: u16,
    /// Exact NDV retained by the progressively merged global domain.
    pub max_global_exact_values: u32,
    /// Exact NDV retained independently by each local builder.
    pub max_local_exact_values: u32,
    pub max_range_value_bytes: u32,
    pub max_dense_bits: u32,
    /// Rows expected to probe the frozen membership representation. This is
    /// advisory physical work, never a correctness or capacity bound.
    pub expected_probe_rows: u64,
    pub mutable_bytes_upper: u64,
    pub freeze_additional_bytes_upper: u64,
    pub peak_memory_bytes: u64,
}

impl RuntimeFilterResourceContract {
    /// Maximum typed payload retained by one complete exact domain. Capacity
    /// is derived from the physical key width so the admission contract has
    /// one stable memory meaning across i32, i64, and i128 keys.
    pub const MAX_EXACT_VALUE_BYTES: u64 = 2 * 1024 * 1024;
    pub const MAX_DENSE_BITS: u32 = 64 * 1024 * 1024;
    pub const MAX_RANGE_VALUE_BYTES: u32 = 4 * 1024;

    pub fn for_keys(key_types: &[LogicalType], max_local_builders: u16) -> Result<Self> {
        Self::for_probe_rows(key_types, max_local_builders, 0)
    }

    pub fn for_probe_rows(
        key_types: &[LogicalType],
        max_local_builders: u16,
        expected_probe_rows: u64,
    ) -> Result<Self> {
        let keys = key_types
            .iter()
            .map(|logical_type| match logical_type {
                LogicalType::Integer | LogicalType::Date => {
                    RuntimeFilterKeyRepresentation::ExactI32
                }
                LogicalType::BigInt
                | LogicalType::Decimal {
                    precision: 0..=18, ..
                } => RuntimeFilterKeyRepresentation::ExactI64,
                LogicalType::Decimal { .. } => RuntimeFilterKeyRepresentation::ExactI128,
                LogicalType::Varchar | LogicalType::VarcharCollation(_) => {
                    RuntimeFilterKeyRepresentation::Range
                }
                LogicalType::Boolean
                | LogicalType::TinyInt
                | LogicalType::SmallInt
                | LogicalType::HugeInt
                | LogicalType::UTinyInt
                | LogicalType::USmallInt
                | LogicalType::UInteger
                | LogicalType::UBigInt
                | LogicalType::UHugeInt
                | LogicalType::Float
                | LogicalType::Double
                | LogicalType::Uuid
                | LogicalType::Timestamp
                | LogicalType::TimestampTz
                | LogicalType::Time
                | LogicalType::Interval => RuntimeFilterKeyRepresentation::Range,
                _ => RuntimeFilterKeyRepresentation::Disabled,
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let max_local_builders = max_local_builders.max(1);
        let exact_width = keys
            .iter()
            .try_fold(0u64, |total, key| {
                total.checked_add(u64::try_from(key.value_width()).unwrap_or(u64::MAX))
            })
            .ok_or_else(|| paro_error::internal("runtime-filter key width overflow"))?;
        let max_global_exact_values = u32::try_from(
            Self::MAX_EXACT_VALUE_BYTES
                .checked_div(exact_width.max(1))
                .unwrap_or(0)
                .clamp(1, u64::from(u32::MAX)),
        )
        .map_err(|_| paro_error::internal("runtime-filter exact capacity overflow"))?;
        let max_local_exact_values = max_global_exact_values
            .div_ceil(u32::from(max_local_builders))
            .max(1);
        // Every local domain and the progressively merged global domain may
        // coexist. Their budgets are distinct executable capabilities.
        let mutable_bytes_upper = exact_width
            .checked_mul(
                u64::from(max_local_exact_values)
                    .checked_mul(u64::from(max_local_builders))
                    .and_then(|bytes| bytes.checked_add(u64::from(max_global_exact_values)))
                    .ok_or_else(|| paro_error::internal("runtime-filter local budget overflow"))?,
            )
            .ok_or_else(|| paro_error::internal("runtime-filter mutable memory overflow"))?;
        let dense_bytes = u64::from(Self::MAX_DENSE_BITS).div_ceil(u64::BITS as u64) * 8;
        let exact_key_count = keys.iter().filter(|key| key.is_exact()).count() as u64;
        let range_key_count = keys
            .iter()
            .filter(|key| **key == RuntimeFilterKeyRepresentation::Range)
            .count() as u64;
        // Freeze first materializes a typed transfer vector, then the final
        // sparse or dense representation, while mutable builders still live.
        let transfer_bytes = exact_width
            .checked_mul(u64::from(max_global_exact_values))
            .ok_or_else(|| paro_error::internal("runtime-filter freeze scratch overflow"))?;
        let frozen_bytes = keys
            .iter()
            .try_fold(0u64, |total, key| {
                let bytes = match key {
                    RuntimeFilterKeyRepresentation::Disabled
                    | RuntimeFilterKeyRepresentation::Range => 0,
                    // The sorted canonical domain remains resident beside an
                    // optional dense point-lookup accelerator. This makes
                    // interval and enumeration work proportional to member
                    // count rather than to gaps in the address space.
                    _ => dense_bytes.saturating_add(
                        u64::try_from(key.value_width())
                            .unwrap_or(u64::MAX)
                            .saturating_mul(u64::from(max_global_exact_values)),
                    ),
                };
                total.checked_add(bytes)
            })
            .ok_or_else(|| paro_error::internal("runtime-filter frozen memory overflow"))?;
        let freeze_additional_bytes_upper = transfer_bytes
            .checked_add(frozen_bytes)
            .ok_or_else(|| paro_error::internal("runtime-filter freeze peak overflow"))?;
        // Every local builder and the progressively merged global builder may
        // simultaneously own two bounded range endpoints.
        let range_metadata = range_key_count
            .checked_mul(u64::from(max_local_builders).saturating_add(1))
            .and_then(|bytes| bytes.checked_mul(2))
            .and_then(|bytes| bytes.checked_mul(u64::from(Self::MAX_RANGE_VALUE_BYTES) + 64))
            .ok_or_else(|| paro_error::internal("runtime-filter range memory overflow"))?;
        let peak_memory_bytes = mutable_bytes_upper
            .checked_add(freeze_additional_bytes_upper)
            .and_then(|bytes| bytes.checked_add(range_metadata))
            .ok_or_else(|| paro_error::internal("runtime-filter peak memory overflow"))?;
        Ok(Self {
            capability: if exact_key_count == 0 && range_key_count == 0 {
                RuntimeFilterCapability::Disabled
            } else if exact_key_count == 0 {
                RuntimeFilterCapability::Range
            } else if exact_key_count == 1 && keys.len() == 1 {
                RuntimeFilterCapability::AdaptiveExactMembership
            } else {
                RuntimeFilterCapability::AdaptivePerKeyMembership
            },
            keys,
            max_local_builders,
            max_global_exact_values,
            max_local_exact_values,
            max_range_value_bytes: Self::MAX_RANGE_VALUE_BYTES,
            max_dense_bits: Self::MAX_DENSE_BITS,
            expected_probe_rows,
            mutable_bytes_upper,
            freeze_additional_bytes_upper,
            peak_memory_bytes,
        })
    }

    /// Whether execution can initially represent the complete tuple domain
    /// as one exact membership set. This deliberately excludes composite
    /// equality keys: independent per-column sets are only a superset of the
    /// build tuples and therefore cannot prove a unique-key survivor bound.
    pub fn has_exact_single_key_representation(&self) -> bool {
        matches!(
            self.keys.as_ref(),
            [RuntimeFilterKeyRepresentation::ExactI32
                | RuntimeFilterKeyRepresentation::ExactI64
                | RuntimeFilterKeyRepresentation::ExactI128]
        )
    }

    /// Whether all executions admitted under this contract retain exact
    /// membership. A total NDV proof must fit both the global domain and one
    /// local builder because scheduler skew can route the complete build to a
    /// single worker.
    pub fn guarantees_exact_single_key(&self, build_ndv_hard_upper: Option<u64>) -> bool {
        let Some(ndv) = build_ndv_hard_upper else {
            return false;
        };
        self.has_exact_single_key_representation()
            && ndv <= u64::from(self.max_global_exact_values)
            && ndv <= u64::from(self.max_local_exact_values)
    }

    /// Whether the expected build domain fits the aggregate exact-membership
    /// capacity of the admitted builders. This is an expected-cost signal,
    /// not an execution proof: skew or stale statistics may still make a
    /// local builder degrade to its range representation.
    pub fn expects_exact_single_key(&self, build_ndv_expected: f64) -> bool {
        if !build_ndv_expected.is_finite() || build_ndv_expected < 0.0 {
            return false;
        }
        let aggregate_local_capacity = u64::from(self.max_local_exact_values)
            .saturating_mul(u64::from(self.max_local_builders));
        self.has_exact_single_key_representation()
            && build_ndv_expected.ceil()
                <= u64::from(self.max_global_exact_values).min(aggregate_local_capacity) as f64
    }

    pub fn validate(&self, key_count: usize) -> Result<()> {
        let exact_key_count = self.keys.iter().filter(|key| key.is_exact()).count();
        let range_key_count = self
            .keys
            .iter()
            .filter(|key| **key == RuntimeFilterKeyRepresentation::Range)
            .count();
        let expected_capability = if exact_key_count == 0 && range_key_count == 0 {
            RuntimeFilterCapability::Disabled
        } else if exact_key_count == 0 {
            RuntimeFilterCapability::Range
        } else if exact_key_count == 1 && self.keys.len() == 1 {
            RuntimeFilterCapability::AdaptiveExactMembership
        } else {
            RuntimeFilterCapability::AdaptivePerKeyMembership
        };
        if self.keys.len() != key_count
            || self.max_local_builders == 0
            || self.max_global_exact_values == 0
            || self.max_local_exact_values == 0
            || self.max_range_value_bytes == 0
            || self
                .max_local_exact_values
                .saturating_mul(u32::from(self.max_local_builders))
                < self.max_global_exact_values
            || self.capability != expected_capability
            || self
                .mutable_bytes_upper
                .saturating_add(self.freeze_additional_bytes_upper)
                > self.peak_memory_bytes
        {
            return Err(paro_error::internal(
                "invalid runtime-filter resource contract",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_blocking_task_declares_an_executable_floor() {
        let contract = ExecutionMemoryContract {
            fixed_non_revocable_bytes: 0,
            fixed_scratch_bytes: BLOCKING_FIXED_SCRATCH_BYTES,
            per_task_scratch_bytes: BLOCKING_PER_TASK_SCRATCH_BYTES,
            max_concurrent_tasks: 1,
            revocable_minimum_bytes: 0,
            revocable_target_bytes: 0,
            spill_buffer_minimum_bytes: SPILL_BUFFER_MINIMUM_BYTES,
        };
        let floor = contract.minimum_memory_bytes().unwrap();

        assert!(floor > 512 * 1024);
        assert!(floor <= 1024 * 1024);
    }

    #[test]
    fn runtime_filter_exactness_matches_the_executable_representation() {
        let integer = RuntimeFilterResourceContract::for_keys(&[LogicalType::Integer], 4)
            .expect("integer contract");
        let string = RuntimeFilterResourceContract::for_keys(&[LogicalType::Varchar], 4)
            .expect("string contract");
        let composite = RuntimeFilterResourceContract::for_keys(
            &[LogicalType::Integer, LogicalType::Integer],
            4,
        )
        .expect("composite contract");

        assert!(integer.has_exact_single_key_representation());
        assert!(!string.has_exact_single_key_representation());
        assert!(!composite.has_exact_single_key_representation());
        assert_eq!(integer.max_global_exact_values, 524_288);
        assert_eq!(integer.max_local_exact_values, 131_072);
        assert!(integer.guarantees_exact_single_key(Some(131_072)));
        assert!(!integer.guarantees_exact_single_key(Some(131_073)));
        assert!(!integer.guarantees_exact_single_key(None));
        assert!(integer.expects_exact_single_key(524_288.0));
        assert!(!integer.expects_exact_single_key(524_289.0));
        assert!(!integer.expects_exact_single_key(f64::NAN));
        assert_eq!(string.capability, RuntimeFilterCapability::Range);
        assert_eq!(
            RuntimeFilterResourceContract::for_keys(&[LogicalType::Blob], 4)
                .expect("blob contract")
                .capability,
            RuntimeFilterCapability::Disabled
        );
        integer.validate(1).expect("valid contract");
        assert_eq!(integer.max_local_builders, 4);
        assert!(integer.peak_memory_bytes >= integer.mutable_bytes_upper);
    }
}
