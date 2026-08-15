// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::predicate_column::PredicateColumnBatch;
use super::segment_predicate::ComparisonOperator;
use crate::index::{
    FixedMembership, FixedMembershipKind, FixedMembershipSet, FixedMembershipValue,
    FixedMembershipView,
};
use crate::rowset::row_id::validate_predicate_batch_rows;
use crate::rowset::BatchRowOrdinal;
use paro_common::error::{self as paro_error, Result};

pub(super) trait FixedPhysical: Copy + Ord + FixedMembershipValue {
    fn from_le(value: Self) -> Self;
}

#[derive(Clone, Copy)]
pub(super) struct FixedBound<T> {
    pub(super) value: T,
    pub(super) inclusive: bool,
}

pub(super) struct FixedConjunction<T> {
    pub(super) equality: Option<T>,
    pub(super) lower: Option<FixedBound<T>>,
    pub(super) upper: Option<FixedBound<T>>,
    inclusions: Option<FixedMembershipSet<T>>,
    exclusions: Vec<T>,
    contradiction: bool,
}

enum FixedKernelShape<'a, T> {
    Contradiction,
    Equality(T),
    Membership(FixedMembershipView<'a, T>),
    Bounds {
        lower: Option<FixedBound<T>>,
        upper: Option<FixedBound<T>>,
    },
    General(&'a FixedConjunction<T>),
}

impl FixedPhysical for i32 {
    #[inline]
    fn from_le(value: Self) -> Self {
        i32::from_le(value)
    }
}

impl FixedPhysical for i64 {
    #[inline]
    fn from_le(value: Self) -> Self {
        i64::from_le(value)
    }
}

impl FixedPhysical for i128 {
    #[inline]
    fn from_le(value: Self) -> Self {
        i128::from_le(value)
    }
}

impl<T: FixedPhysical> FixedConjunction<T> {
    pub(super) fn new(operator: ComparisonOperator, rhs: T) -> Self {
        let mut conjunction = Self {
            equality: None,
            lower: None,
            upper: None,
            inclusions: None,
            exclusions: Vec::new(),
            contradiction: false,
        };
        conjunction.add(operator, rhs);
        conjunction
    }

    pub(super) fn from_in(values: Vec<T>) -> Self {
        let inclusions = FixedMembershipSet::from_values(values);
        let contradiction = inclusions.is_empty();
        Self {
            equality: None,
            lower: None,
            upper: None,
            inclusions: Some(inclusions),
            exclusions: Vec::new(),
            contradiction,
        }
    }

    pub(super) fn from_range(lower: T, upper: T) -> Self {
        let mut conjunction = Self::new(ComparisonOperator::GreaterThanOrEqual, lower);
        conjunction.add(ComparisonOperator::LessThanOrEqual, upper);
        conjunction
    }

    fn from_membership(inclusions: FixedMembershipSet<T>) -> Self {
        let contradiction = inclusions.is_empty();
        Self {
            equality: None,
            lower: None,
            upper: None,
            inclusions: Some(inclusions),
            exclusions: Vec::new(),
            contradiction,
        }
    }

    fn add(&mut self, operator: ComparisonOperator, rhs: T) {
        match operator {
            ComparisonOperator::Equal => match self.equality {
                Some(existing) if existing != rhs => self.contradiction = true,
                Some(_) => {}
                None => self.equality = Some(rhs),
            },
            ComparisonOperator::NotEqual => {
                if !self.exclusions.contains(&rhs) {
                    self.exclusions.push(rhs);
                }
            }
            ComparisonOperator::LessThan | ComparisonOperator::LessThanOrEqual => {
                Self::tighten_upper(
                    &mut self.upper,
                    FixedBound {
                        value: rhs,
                        inclusive: matches!(operator, ComparisonOperator::LessThanOrEqual),
                    },
                );
            }
            ComparisonOperator::GreaterThan | ComparisonOperator::GreaterThanOrEqual => {
                Self::tighten_lower(
                    &mut self.lower,
                    FixedBound {
                        value: rhs,
                        inclusive: matches!(operator, ComparisonOperator::GreaterThanOrEqual),
                    },
                );
            }
        }
        self.validate();
    }

    fn merge(&mut self, other: &mut Self) {
        if other.contradiction {
            self.contradiction = true;
        }
        if let Some(value) = other.equality {
            self.add(ComparisonOperator::Equal, value);
        }
        if let Some(bound) = other.lower {
            self.add(
                if bound.inclusive {
                    ComparisonOperator::GreaterThanOrEqual
                } else {
                    ComparisonOperator::GreaterThan
                },
                bound.value,
            );
        }
        if let Some(bound) = other.upper {
            self.add(
                if bound.inclusive {
                    ComparisonOperator::LessThanOrEqual
                } else {
                    ComparisonOperator::LessThan
                },
                bound.value,
            );
        }
        if let Some(incoming) = other.inclusions.take() {
            if let Some(existing) = &mut self.inclusions {
                existing.intersect(&incoming);
            } else {
                self.inclusions = Some(incoming);
            }
        }
        for value in other.exclusions.drain(..) {
            self.add(ComparisonOperator::NotEqual, value);
        }
        self.validate();
    }

    fn tighten_lower(current: &mut Option<FixedBound<T>>, incoming: FixedBound<T>) {
        if current.is_none_or(|existing| {
            incoming.value > existing.value
                || (incoming.value == existing.value && !incoming.inclusive && existing.inclusive)
        }) {
            *current = Some(incoming);
        }
    }

    fn tighten_upper(current: &mut Option<FixedBound<T>>, incoming: FixedBound<T>) {
        if current.is_none_or(|existing| {
            incoming.value < existing.value
                || (incoming.value == existing.value && !incoming.inclusive && existing.inclusive)
        }) {
            *current = Some(incoming);
        }
    }

    fn validate(&mut self) {
        if let (Some(lower), Some(upper)) = (self.lower, self.upper) {
            self.contradiction |= lower.value > upper.value
                || (lower.value == upper.value && (!lower.inclusive || !upper.inclusive));
        }
        if let Some(value) = self.equality {
            self.contradiction |= self.exclusions.contains(&value)
                || self
                    .inclusions
                    .as_ref()
                    .is_some_and(|values| !values.contains(value))
                || self.lower.is_some_and(|bound| {
                    value < bound.value || (value == bound.value && !bound.inclusive)
                })
                || self.upper.is_some_and(|bound| {
                    value > bound.value || (value == bound.value && !bound.inclusive)
                });
        }
        if let Some(values) = &mut self.inclusions {
            let equality = self.equality;
            let lower = self.lower;
            let upper = self.upper;
            let exclusions = &self.exclusions;
            let first = values.first();
            let last = values.last();
            let equality_restricts =
                equality.is_some_and(|expected| values.len() != 1 || first != Some(expected));
            let lower_restricts = lower.is_some_and(|bound| {
                first.is_some_and(|value| {
                    value < bound.value || (value == bound.value && !bound.inclusive)
                })
            });
            let upper_restricts = upper.is_some_and(|bound| {
                last.is_some_and(|value| {
                    value > bound.value || (value == bound.value && !bound.inclusive)
                })
            });
            let exclusion_restricts = exclusions.iter().any(|value| values.contains(*value));
            if equality_restricts || lower_restricts || upper_restricts || exclusion_restricts {
                values.retain(|value| {
                    equality.is_none_or(|expected| value == expected)
                        && lower.is_none_or(|bound| {
                            value > bound.value || (bound.inclusive && value == bound.value)
                        })
                        && upper.is_none_or(|bound| {
                            value < bound.value || (bound.inclusive && value == bound.value)
                        })
                        && !exclusions.contains(&value)
                });
            }
            self.contradiction |= values.is_empty();
        }
    }

    #[inline]
    pub(super) fn matches(&self, value: T) -> bool {
        if self.contradiction {
            return false;
        }
        // Validation canonicalizes every other constraint into the inclusion
        // set. Once present, membership is therefore both necessary and
        // sufficient and the hot loop need not re-check bounds/exclusions.
        if let Some(values) = &self.inclusions {
            return values.contains(value);
        }
        self.equality.is_none_or(|expected| value == expected)
            && self.lower.is_none_or(|bound| {
                value > bound.value || (bound.inclusive && value == bound.value)
            })
            && self.upper.is_none_or(|bound| {
                value < bound.value || (bound.inclusive && value == bound.value)
            })
            && !self.exclusions.contains(&value)
    }

    /// Static fallback ordering for conjunction evaluation when no histogram
    /// selectivity is available. Positive membership and bounded predicates
    /// usually discard rows; exclusions usually retain them. The second field
    /// keeps smaller membership sets ahead of larger sets within that class.
    pub(super) fn evaluation_priority(&self) -> (u8, usize) {
        if self.contradiction {
            return (0, 0);
        }
        if self.equality.is_some() {
            return (1, 1);
        }
        if let Some(values) = &self.inclusions {
            return (2, values.len());
        }
        match (self.lower.is_some(), self.upper.is_some()) {
            (true, true) => (3, 0),
            (true, false) | (false, true) => (4, 0),
            (false, false) if !self.exclusions.is_empty() => (6, self.exclusions.len()),
            (false, false) => (7, 0),
        }
    }

    /// Canonical execution shape shared by scalar and architecture-specific
    /// kernels. The exhaustive field destructuring intentionally makes adding
    /// new conjunction semantics a compile-time obligation here.
    fn execution_shape(&self) -> FixedKernelShape<'_, T> {
        let Self {
            equality,
            lower,
            upper,
            inclusions,
            exclusions,
            contradiction,
        } = self;
        if *contradiction {
            FixedKernelShape::Contradiction
        } else if let Some(value) = equality {
            FixedKernelShape::Equality(*value)
        } else if let Some(values) = inclusions {
            FixedKernelShape::Membership(values.view())
        } else if exclusions.is_empty() {
            FixedKernelShape::Bounds {
                lower: *lower,
                upper: *upper,
            }
        } else {
            FixedKernelShape::General(self)
        }
    }
}

pub(super) enum FixedComparisonValues {
    I32(FixedConjunction<i32>),
    I64(FixedConjunction<i64>),
    I128(FixedConjunction<i128>),
}

impl FixedComparisonValues {
    pub(super) fn from_membership(values: FixedMembership) -> Self {
        match values.into_kind() {
            FixedMembershipKind::I32(values) => {
                Self::I32(FixedConjunction::from_membership(values))
            }
            FixedMembershipKind::I64(values) => {
                Self::I64(FixedConjunction::from_membership(values))
            }
            FixedMembershipKind::I128(values) => {
                Self::I128(FixedConjunction::from_membership(values))
            }
        }
    }

    pub(super) fn physical_width(&self) -> usize {
        match self {
            Self::I32(_) => std::mem::size_of::<i32>(),
            Self::I64(_) => std::mem::size_of::<i64>(),
            Self::I128(_) => std::mem::size_of::<i128>(),
        }
    }

    pub(super) fn extend_same_type(&mut self, other: &mut Self) -> bool {
        match (self, other) {
            (Self::I32(existing), Self::I32(incoming)) => existing.merge(incoming),
            (Self::I64(existing), Self::I64(incoming)) => existing.merge(incoming),
            (Self::I128(existing), Self::I128(incoming)) => existing.merge(incoming),
            _ => return false,
        }
        true
    }

    pub(super) fn filter_batch(
        &self,
        batch: &PredicateColumnBatch,
        rows: usize,
        selection: &mut Vec<BatchRowOrdinal>,
        seed: bool,
    ) -> Result<()> {
        validate_predicate_batch_rows(rows)?;
        match self {
            Self::I32(kernel) => {
                if try_filter_seed_i32_batch(batch, kernel, rows, selection, seed) {
                    return Ok(());
                }
                filter_fixed_batch(batch, kernel, rows, selection, seed)
            }
            Self::I64(kernel) => {
                if try_filter_seed_i64_batch(batch, kernel, rows, selection, seed) {
                    return Ok(());
                }
                filter_fixed_batch(batch, kernel, rows, selection, seed)
            }
            Self::I128(kernel) => filter_fixed_batch(batch, kernel, rows, selection, seed),
        }
    }

    pub(super) fn evaluation_priority(&self) -> (u8, usize) {
        match self {
            Self::I32(kernel) => kernel.evaluation_priority(),
            Self::I64(kernel) => kernel.evaluation_priority(),
            Self::I128(kernel) => kernel.evaluation_priority(),
        }
    }
}

trait SimdInteger: FixedPhysical {
    const MIN: Self;
    const MAX: Self;

    fn checked_increment(self) -> Option<Self>;
    fn checked_decrement(self) -> Option<Self>;
}

impl SimdInteger for i32 {
    const MIN: Self = i32::MIN;
    const MAX: Self = i32::MAX;

    fn checked_increment(self) -> Option<Self> {
        self.checked_add(1)
    }

    fn checked_decrement(self) -> Option<Self> {
        self.checked_sub(1)
    }
}

impl SimdInteger for i64 {
    const MIN: Self = i64::MIN;
    const MAX: Self = i64::MAX;

    fn checked_increment(self) -> Option<Self> {
        self.checked_add(1)
    }

    fn checked_decrement(self) -> Option<Self> {
        self.checked_sub(1)
    }
}

/// Normalize every open/closed, one/two-sided integer range to one inclusive
/// shape. Width-specific SIMD code therefore implements only lane loading and
/// comparison; adding a bound shape cannot create an architecture/type hole.
fn inclusive_simd_bounds<T: SimdInteger>(kernel: &FixedConjunction<T>) -> Option<Option<(T, T)>> {
    match kernel.execution_shape() {
        FixedKernelShape::Bounds { lower, upper } => {
            let lower = match lower {
                None => Some(T::MIN),
                Some(lower) if lower.inclusive => Some(lower.value),
                Some(lower) => lower.value.checked_increment(),
            };
            let upper = match upper {
                None => Some(T::MAX),
                Some(upper) if upper.inclusive => Some(upper.value),
                Some(upper) => upper.value.checked_decrement(),
            };
            let (Some(lower), Some(upper)) = (lower, upper) else {
                return Some(None);
            };
            Some((lower <= upper).then_some((lower, upper)))
        }
        _ => None,
    }
}

fn try_filter_seed_i64_batch(
    batch: &PredicateColumnBatch,
    kernel: &FixedConjunction<i64>,
    rows: usize,
    selection: &mut Vec<BatchRowOrdinal>,
    seed: bool,
) -> bool {
    let PredicateColumnBatch::Raw(batch) = batch else {
        return false;
    };
    if !seed || batch.nulls.is_some() {
        return false;
    }
    let Some(bounds) = inclusive_simd_bounds(kernel) else {
        return false;
    };
    let Some((lower, upper)) = bounds else {
        selection.clear();
        return true;
    };

    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    {
        unsafe {
            filter_i64_range_inclusive_neon(batch.data.as_ptr(), lower, upper, rows, selection)
        }
    }

    #[cfg(all(target_arch = "x86_64", target_endian = "little"))]
    {
        if std::arch::is_x86_feature_detected!("avx2") {
            return unsafe {
                filter_i64_range_inclusive_avx2(batch.data.as_ptr(), lower, upper, rows, selection)
            };
        }
        return false;
    }

    #[cfg(not(any(
        all(target_arch = "aarch64", target_endian = "little"),
        all(target_arch = "x86_64", target_endian = "little")
    )))]
    {
        false
    }
}

#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
const fn neon_compaction_table() -> [[u8; 16]; 16] {
    let mut table = [[0u8; 16]; 16];
    let mut mask = 0usize;
    while mask < table.len() {
        let mut output_lane = 0usize;
        let mut input_lane = 0usize;
        while input_lane < 4 {
            if mask & (1 << input_lane) != 0 {
                let mut byte = 0usize;
                while byte < 4 {
                    table[mask][output_lane * 4 + byte] = (input_lane * 4 + byte) as u8;
                    byte += 1;
                }
                output_lane += 1;
            }
            input_lane += 1;
        }
        mask += 1;
    }
    table
}

#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
static NEON_COMPACTION_TABLE: [[u8; 16]; 16] = neon_compaction_table();

/// Compact four row ordinals with one table shuffle and one full-vector store.
///
/// `matched` contains all-zero/all-one lanes. The caller reserves at least `rows + 4`
/// elements, so the unconditional 16-byte store may initialize up to four
/// slots beyond the logical selection. Only `mask.count_ones()` values are
/// exposed by the final `set_len`; every exposed lane comes from a set mask
/// bit and is therefore initialized with the corresponding input ordinal.
#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
#[inline(always)]
unsafe fn compact_ordinals_neon(
    matched: core::arch::aarch64::uint32x4_t,
    ordinals: core::arch::aarch64::uint32x4_t,
    output: *mut BatchRowOrdinal,
    written: usize,
) -> usize {
    use core::arch::aarch64::{
        vaddvq_u32, vandq_u32, vld1q_u32, vld1q_u8, vqtbl1q_u8, vreinterpretq_u32_u8,
        vreinterpretq_u8_u32, vst1q_u32,
    };

    let weights = unsafe { vld1q_u32([1u32, 2, 4, 8].as_ptr()) };
    let mask = unsafe { vaddvq_u32(vandq_u32(matched, weights)) } as usize;
    let shuffle = unsafe { vld1q_u8(NEON_COMPACTION_TABLE[mask].as_ptr()) };
    let packed =
        unsafe { vreinterpretq_u32_u8(vqtbl1q_u8(vreinterpretq_u8_u32(ordinals), shuffle)) };
    unsafe { vst1q_u32(output.add(written).cast::<u32>(), packed) };
    mask.count_ones() as usize
}

#[cfg(all(target_arch = "x86_64", target_endian = "little"))]
const fn avx2_compaction_table() -> [[i32; 8]; 256] {
    let mut table = [[0i32; 8]; 256];
    let mut mask = 0usize;
    while mask < table.len() {
        let mut output_lane = 0usize;
        let mut input_lane = 0usize;
        while input_lane < 8 {
            if mask & (1 << input_lane) != 0 {
                table[mask][output_lane] = input_lane as i32;
                output_lane += 1;
            }
            input_lane += 1;
        }
        mask += 1;
    }
    table
}

#[cfg(all(target_arch = "x86_64", target_endian = "little"))]
static AVX2_COMPACTION_TABLE: [[i32; 8]; 256] = avx2_compaction_table();

/// AVX2 counterpart of `compact_ordinals_neon`. Callers reserve `rows + 8`
/// because this helper always writes one full 8-lane vector and exposes only
/// the lanes selected by `matched_mask`.
#[cfg(all(target_arch = "x86_64", target_endian = "little"))]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn compact_ordinals_avx2(
    matched_mask: u32,
    ordinals: core::arch::x86_64::__m256i,
    output: *mut BatchRowOrdinal,
    written: usize,
) -> usize {
    use core::arch::x86_64::{
        __m256i, _mm256_loadu_si256, _mm256_permutevar8x32_epi32, _mm256_storeu_si256,
    };

    let mask = (matched_mask & 0xff) as usize;
    let permutation =
        unsafe { _mm256_loadu_si256(AVX2_COMPACTION_TABLE[mask].as_ptr().cast::<__m256i>()) };
    let packed = unsafe { _mm256_permutevar8x32_epi32(ordinals, permutation) };
    unsafe { _mm256_storeu_si256(output.add(written).cast::<__m256i>(), packed) };
    mask.count_ones() as usize
}

#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
unsafe fn filter_i64_range_inclusive_neon(
    input: *const u8,
    lower: i64,
    upper: i64,
    rows: usize,
    selection: &mut Vec<BatchRowOrdinal>,
) -> bool {
    use core::arch::aarch64::{
        vaddq_u32, vandq_u64, vcgeq_s64, vcleq_s64, vcombine_u32, vdup_n_u32, vdupq_n_s64,
        vdupq_n_u32, vld1q_u32, vld1q_u8, vmovn_u64, vreinterpretq_s64_u8,
    };

    selection.reserve(rows + 4);
    let start = selection.len();
    let output = selection
        .spare_capacity_mut()
        .as_mut_ptr()
        .cast::<BatchRowOrdinal>();
    let lower_vector = unsafe { vdupq_n_s64(lower) };
    let upper_vector = unsafe { vdupq_n_s64(upper) };
    let mut ordinals = unsafe { vld1q_u32([0u32, 1, 2, 3].as_ptr()) };
    let ordinal_step = unsafe { vdupq_n_u32(2) };
    let mut row = 0usize;
    let mut written = 0usize;
    while row + 2 <= rows {
        let bytes = unsafe { vld1q_u8(input.add(row * std::mem::size_of::<i64>())) };
        let values = unsafe { vreinterpretq_s64_u8(bytes) };
        let above_lower = unsafe { vcgeq_s64(values, lower_vector) };
        let below_upper = unsafe { vcleq_s64(values, upper_vector) };
        let matched = unsafe { vandq_u64(above_lower, below_upper) };
        let matched = unsafe { vcombine_u32(vmovn_u64(matched), vdup_n_u32(0)) };
        written += unsafe { compact_ordinals_neon(matched, ordinals, output, written) };
        ordinals = unsafe { vaddq_u32(ordinals, ordinal_step) };
        row += 2;
    }
    if row < rows {
        let value = i64::from_le(unsafe {
            input
                .add(row * std::mem::size_of::<i64>())
                .cast::<i64>()
                .read_unaligned()
        });
        if value >= lower && value <= upper {
            unsafe { output.add(written).write(BatchRowOrdinal::from_index(row)) };
            written += 1;
        }
    }
    unsafe { selection.set_len(start + written) };
    true
}

#[cfg(all(target_arch = "x86_64", target_endian = "little"))]
#[target_feature(enable = "avx2")]
unsafe fn filter_i64_range_inclusive_avx2(
    input: *const u8,
    lower: i64,
    upper: i64,
    rows: usize,
    selection: &mut Vec<BatchRowOrdinal>,
) -> bool {
    use core::arch::x86_64::{
        __m256i, _mm256_add_epi32, _mm256_castsi256_pd, _mm256_cmpgt_epi64, _mm256_loadu_si256,
        _mm256_movemask_pd, _mm256_or_si256, _mm256_set1_epi32, _mm256_set1_epi64x,
    };

    selection.reserve(rows + 8);
    let start = selection.len();
    let output = selection
        .spare_capacity_mut()
        .as_mut_ptr()
        .cast::<BatchRowOrdinal>();
    let lower_vector = unsafe { _mm256_set1_epi64x(lower) };
    let upper_vector = unsafe { _mm256_set1_epi64x(upper) };
    let mut ordinals = unsafe { _mm256_loadu_si256([0i32, 1, 2, 3, 4, 5, 6, 7].as_ptr().cast()) };
    let ordinal_step = unsafe { _mm256_set1_epi32(4) };
    let mut row = 0usize;
    let mut written = 0usize;
    while row + 4 <= rows {
        let values = unsafe {
            _mm256_loadu_si256(
                input
                    .add(row * std::mem::size_of::<i64>())
                    .cast::<__m256i>(),
            )
        };
        let below_lower = unsafe { _mm256_cmpgt_epi64(lower_vector, values) };
        let above_upper = unsafe { _mm256_cmpgt_epi64(values, upper_vector) };
        let rejected = unsafe {
            _mm256_movemask_pd(_mm256_castsi256_pd(_mm256_or_si256(
                below_lower,
                above_upper,
            ))) as u32
        };
        written += unsafe { compact_ordinals_avx2(!rejected & 0x0f, ordinals, output, written) };
        ordinals = unsafe { _mm256_add_epi32(ordinals, ordinal_step) };
        row += 4;
    }
    while row < rows {
        let value = i64::from_le(unsafe {
            input
                .add(row * std::mem::size_of::<i64>())
                .cast::<i64>()
                .read_unaligned()
        });
        if value >= lower && value <= upper {
            unsafe { output.add(written).write(BatchRowOrdinal::from_index(row)) };
            written += 1;
        }
        row += 1;
    }
    unsafe { selection.set_len(start + written) };
    true
}

fn try_filter_seed_i32_batch(
    batch: &PredicateColumnBatch,
    kernel: &FixedConjunction<i32>,
    rows: usize,
    selection: &mut Vec<BatchRowOrdinal>,
    seed: bool,
) -> bool {
    let PredicateColumnBatch::Raw(batch) = batch else {
        return false;
    };
    if !seed || batch.nulls.is_some() {
        return false;
    }

    let Some(bounds) = inclusive_simd_bounds(kernel) else {
        return false;
    };
    let Some((lower, upper)) = bounds else {
        selection.clear();
        return true;
    };

    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    {
        unsafe {
            filter_i32_range_inclusive_neon(batch.data.as_ptr(), lower, upper, rows, selection)
        }
    }

    #[cfg(all(target_arch = "x86_64", target_endian = "little"))]
    {
        if std::arch::is_x86_feature_detected!("avx2") {
            return unsafe {
                filter_i32_range_inclusive_avx2(batch.data.as_ptr(), lower, upper, rows, selection)
            };
        }
        return false;
    }

    #[cfg(not(any(
        all(target_arch = "aarch64", target_endian = "little"),
        all(target_arch = "x86_64", target_endian = "little")
    )))]
    {
        false
    }
}

#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
unsafe fn filter_i32_range_inclusive_neon(
    input: *const u8,
    lower: i32,
    upper: i32,
    rows: usize,
    selection: &mut Vec<BatchRowOrdinal>,
) -> bool {
    use core::arch::aarch64::{
        vaddq_u32, vandq_u32, vcgeq_s32, vcleq_s32, vdupq_n_s32, vdupq_n_u32, vld1q_u32, vld1q_u8,
        vreinterpretq_s32_u8,
    };

    selection.reserve(rows + 4);
    let start = selection.len();
    let output = selection
        .spare_capacity_mut()
        .as_mut_ptr()
        .cast::<BatchRowOrdinal>();
    let lower_vector = unsafe { vdupq_n_s32(lower) };
    let upper_vector = unsafe { vdupq_n_s32(upper) };
    let mut ordinals = unsafe { vld1q_u32([0u32, 1, 2, 3].as_ptr()) };
    let ordinal_step = unsafe { vdupq_n_u32(4) };
    let mut row = 0usize;
    let mut written = 0usize;
    while row + 4 <= rows {
        let bytes = unsafe { vld1q_u8(input.add(row * std::mem::size_of::<i32>())) };
        let values = unsafe { vreinterpretq_s32_u8(bytes) };
        let above_lower = unsafe { vcgeq_s32(values, lower_vector) };
        let below_upper = unsafe { vcleq_s32(values, upper_vector) };
        let matched = unsafe { vandq_u32(above_lower, below_upper) };
        written += unsafe { compact_ordinals_neon(matched, ordinals, output, written) };
        ordinals = unsafe { vaddq_u32(ordinals, ordinal_step) };
        row += 4;
    }
    while row < rows {
        let value = i32::from_le(unsafe {
            input
                .add(row * std::mem::size_of::<i32>())
                .cast::<i32>()
                .read_unaligned()
        });
        if value >= lower && value <= upper {
            unsafe { output.add(written).write(BatchRowOrdinal::from_index(row)) };
            written += 1;
        }
        row += 1;
    }
    unsafe { selection.set_len(start + written) };
    true
}
#[cfg(all(target_arch = "x86_64", target_endian = "little"))]
#[target_feature(enable = "avx2")]
unsafe fn filter_i32_range_inclusive_avx2(
    input: *const u8,
    lower: i32,
    upper: i32,
    rows: usize,
    selection: &mut Vec<BatchRowOrdinal>,
) -> bool {
    use core::arch::x86_64::{
        __m256i, _mm256_add_epi32, _mm256_castsi256_ps, _mm256_cmpgt_epi32, _mm256_loadu_si256,
        _mm256_movemask_ps, _mm256_or_si256, _mm256_set1_epi32,
    };

    selection.reserve(rows + 8);
    let start = selection.len();
    let output = selection
        .spare_capacity_mut()
        .as_mut_ptr()
        .cast::<BatchRowOrdinal>();
    let lower_vector = unsafe { _mm256_set1_epi32(lower) };
    let upper_vector = unsafe { _mm256_set1_epi32(upper) };
    let mut ordinals = unsafe { _mm256_loadu_si256([0i32, 1, 2, 3, 4, 5, 6, 7].as_ptr().cast()) };
    let ordinal_step = unsafe { _mm256_set1_epi32(8) };
    let mut row = 0usize;
    let mut written = 0usize;
    while row + 8 <= rows {
        let values = unsafe {
            _mm256_loadu_si256(
                input
                    .add(row * std::mem::size_of::<i32>())
                    .cast::<__m256i>(),
            )
        };
        let below_lower = unsafe { _mm256_cmpgt_epi32(lower_vector, values) };
        let above_upper = unsafe { _mm256_cmpgt_epi32(values, upper_vector) };
        let rejected = unsafe {
            _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_or_si256(
                below_lower,
                above_upper,
            ))) as u32
        };
        written += unsafe { compact_ordinals_avx2(!rejected & 0xff, ordinals, output, written) };
        ordinals = unsafe { _mm256_add_epi32(ordinals, ordinal_step) };
        row += 8;
    }
    while row < rows {
        let value = i32::from_le(unsafe {
            input
                .add(row * std::mem::size_of::<i32>())
                .cast::<i32>()
                .read_unaligned()
        });
        if value >= lower && value <= upper {
            unsafe { output.add(written).write(BatchRowOrdinal::from_index(row)) };
            written += 1;
        }
        row += 1;
    }
    unsafe { selection.set_len(start + written) };
    true
}

fn filter_fixed_batch<T: FixedPhysical>(
    batch: &PredicateColumnBatch,
    kernel: &FixedConjunction<T>,
    rows: usize,
    selection: &mut Vec<BatchRowOrdinal>,
    seed: bool,
) -> Result<()> {
    match batch {
        PredicateColumnBatch::Raw(batch) => {
            let data = batch.data.as_ptr();
            let load = |row_idx: usize| {
                // SAFETY: the caller validates rows * physical width, and the
                // selection contains only indices below rows.
                T::from_le(unsafe {
                    data.add(row_idx * std::mem::size_of::<T>())
                        .cast::<T>()
                        .read_unaligned()
                })
            };
            if let Some(nulls) = &batch.nulls {
                dispatch_fixed_kernel(kernel, rows, selection, seed, load, |row_idx| {
                    nulls[row_idx] == 0
                });
            } else {
                dispatch_fixed_kernel(kernel, rows, selection, seed, load, |_| true);
            }
            Ok(())
        }
        PredicateColumnBatch::StorageDictionary(batch) => {
            let code_matches = (0..batch.dictionary_len())
                .map(|code| {
                    let value = batch.dictionary_value(code);
                    // SAFETY: dictionary batch construction validates every
                    // entry against this predicate's compiled physical width.
                    let value = unsafe { value.as_ptr().cast::<T>().read_unaligned() };
                    kernel.matches(T::from_le(value))
                })
                .collect::<Vec<_>>();
            batch.filter_codes(&code_matches, selection, seed);
            Ok(())
        }
        PredicateColumnBatch::Decoded(vector) => {
            let load = |row_idx: usize| {
                // SAFETY: the compiled physical width matches the decoded
                // vector type and every selected index is below rows.
                unsafe { vector.get_fixed::<T>(row_idx) }
            };
            if vector.validity().all_valid() {
                dispatch_fixed_kernel(kernel, rows, selection, seed, load, |_| true);
            } else {
                dispatch_fixed_kernel(kernel, rows, selection, seed, load, |row_idx| {
                    !vector.is_null(row_idx)
                });
            }
            Ok(())
        }
        PredicateColumnBatch::RawVarlen(_) => Err(paro_error::internal(
            "fixed predicate received a variable-length column batch",
        )),
    }
}

fn dispatch_fixed_kernel<T, L, V>(
    kernel: &FixedConjunction<T>,
    rows: usize,
    selection: &mut Vec<BatchRowOrdinal>,
    seed: bool,
    load: L,
    valid: V,
) where
    T: FixedPhysical,
    L: Fn(usize) -> T + Copy,
    V: Fn(usize) -> bool + Copy,
{
    match kernel.execution_shape() {
        FixedKernelShape::Contradiction => selection.clear(),
        FixedKernelShape::Equality(expected) => {
            filter_selection(rows, selection, seed, load, valid, |value| {
                value == expected
            });
        }
        FixedKernelShape::Membership(inclusions) => match inclusions {
            FixedMembershipView::Sorted(values) => {
                filter_selection(rows, selection, seed, load, valid, |value| {
                    values.binary_search(&value).is_ok()
                });
            }
            FixedMembershipView::Dense { base, span, bits } => {
                filter_selection(rows, selection, seed, load, valid, |value| {
                    let Some(offset) = value.offset_from(base).filter(|offset| *offset < span)
                    else {
                        return false;
                    };
                    bits[offset / u64::BITS as usize] & (1_u64 << (offset % u64::BITS as usize))
                        != 0
                });
            }
        },
        FixedKernelShape::General(kernel) => {
            filter_selection(rows, selection, seed, load, valid, |value| {
                kernel.matches(value)
            });
        }
        FixedKernelShape::Bounds { lower, upper } => match (lower, upper) {
            (None, None) => filter_selection(rows, selection, seed, load, valid, |_| true),
            (Some(lower), None) if lower.inclusive => {
                filter_selection(rows, selection, seed, load, valid, |value| {
                    value >= lower.value
                })
            }
            (Some(lower), None) => filter_selection(rows, selection, seed, load, valid, |value| {
                value > lower.value
            }),
            (None, Some(upper)) if upper.inclusive => {
                filter_selection(rows, selection, seed, load, valid, |value| {
                    value <= upper.value
                })
            }
            (None, Some(upper)) => filter_selection(rows, selection, seed, load, valid, |value| {
                value < upper.value
            }),
            (Some(lower), Some(upper)) => match (lower.inclusive, upper.inclusive) {
                (true, true) => filter_selection(rows, selection, seed, load, valid, |value| {
                    value >= lower.value && value <= upper.value
                }),
                (true, false) => filter_selection(rows, selection, seed, load, valid, |value| {
                    value >= lower.value && value < upper.value
                }),
                (false, true) => filter_selection(rows, selection, seed, load, valid, |value| {
                    value > lower.value && value <= upper.value
                }),
                (false, false) => filter_selection(rows, selection, seed, load, valid, |value| {
                    value > lower.value && value < upper.value
                }),
            },
        },
    }
}

#[inline]
fn filter_selection<T, L, V, P>(
    rows: usize,
    selection: &mut Vec<BatchRowOrdinal>,
    seed: bool,
    load: L,
    valid: V,
    predicate: P,
) where
    T: Copy,
    L: Fn(usize) -> T,
    V: Fn(usize) -> bool,
    P: Fn(T) -> bool,
{
    if seed {
        selection.reserve(rows);
        let start = selection.len();
        let spare = selection.spare_capacity_mut();
        let mut written = 0usize;
        for row_idx in 0..rows {
            if valid(row_idx) && predicate(load(row_idx)) {
                spare[written].write(BatchRowOrdinal::from_index(row_idx));
                written += 1;
            }
        }
        // SAFETY: exactly `written` consecutive entries in the spare capacity
        // were initialized above, and reserve ensured room for `rows` entries.
        unsafe { selection.set_len(start + written) };
        return;
    }

    let mut write_idx = 0;
    for read_idx in 0..selection.len() {
        let row_idx = selection[read_idx].index();
        if valid(row_idx) && predicate(load(row_idx)) {
            selection[write_idx] = BatchRowOrdinal::from_index(row_idx);
            write_idx += 1;
        }
    }
    selection.truncate(write_idx);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simd_compaction_matches_scalar_for_partial_and_full_vectors() {
        for rows in [0usize, 1, 2, 3, 4, 5, 7, 8, 999, 1000] {
            let values = (0..rows)
                .map(|row| ((row * 37 + 11) % 101) as i32)
                .collect::<Vec<_>>();
            let bytes = values
                .iter()
                .copied()
                .flat_map(i32::to_le_bytes)
                .collect::<Vec<_>>();
            let expected = values
                .iter()
                .enumerate()
                .filter_map(|(row, value)| {
                    (20..=70)
                        .contains(value)
                        .then_some(BatchRowOrdinal::from_index(row))
                })
                .collect::<Vec<_>>();

            #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
            let actual = {
                let mut selection = Vec::new();
                assert!(unsafe {
                    filter_i32_range_inclusive_neon(bytes.as_ptr(), 20, 70, rows, &mut selection)
                });
                selection
            };

            #[cfg(all(target_arch = "x86_64", target_endian = "little"))]
            let actual = {
                if !std::arch::is_x86_feature_detected!("avx2") {
                    return;
                }
                let mut selection = Vec::new();
                assert!(unsafe {
                    filter_i32_range_inclusive_avx2(bytes.as_ptr(), 20, 70, rows, &mut selection)
                });
                selection
            };

            #[cfg(not(any(
                all(target_arch = "aarch64", target_endian = "little"),
                all(target_arch = "x86_64", target_endian = "little")
            )))]
            let actual = expected.clone();

            assert_eq!(actual, expected, "row count {rows}");
        }
    }
}
