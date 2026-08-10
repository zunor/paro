// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::predicate_column::PredicateColumnBatch;
use super::segment_predicate::ComparisonOperator;
use crate::index::{
    FixedMembership, FixedMembershipKind, FixedMembershipSet, FixedMembershipValue,
    FixedMembershipView,
};
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
        selection: &mut Vec<usize>,
        seed: bool,
    ) -> Result<()> {
        match self {
            Self::I32(kernel) => {
                if try_filter_seed_i32_batch(batch, kernel, rows, selection, seed) {
                    return Ok(());
                }
                filter_fixed_batch(batch, kernel, rows, selection, seed)
            }
            Self::I64(kernel) => filter_fixed_batch(batch, kernel, rows, selection, seed),
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

#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
fn try_filter_seed_i32_batch(
    batch: &PredicateColumnBatch,
    kernel: &FixedConjunction<i32>,
    rows: usize,
    selection: &mut Vec<usize>,
    seed: bool,
) -> bool {
    use core::arch::aarch64::{vcleq_s32, vdupq_n_s32, vld1q_s32, vst1q_u32};

    let PredicateColumnBatch::Raw(batch) = batch else {
        return false;
    };
    let (None, Some(upper_bound)) = (kernel.lower, kernel.upper) else {
        return false;
    };
    if !seed
        || batch.nulls.is_some()
        || !upper_bound.inclusive
        || kernel.contradiction
        || kernel.equality.is_some()
        || kernel.inclusions.is_some()
        || !kernel.exclusions.is_empty()
    {
        return false;
    }

    selection.reserve(rows);
    let start = selection.len();
    let output = selection.spare_capacity_mut().as_mut_ptr().cast::<usize>();
    let input = batch.data.as_ptr().cast::<i32>();
    let upper_vector = unsafe { vdupq_n_s32(upper_bound.value) };
    let mut row = 0usize;
    let mut written = 0usize;
    while row + 4 <= rows {
        let values = unsafe { vld1q_s32(input.add(row)) };
        let matched = unsafe { vcleq_s32(values, upper_vector) };
        let mut lanes = [0u32; 4];
        unsafe { vst1q_u32(lanes.as_mut_ptr(), matched) };
        if lanes == [u32::MAX; 4] {
            unsafe {
                output.add(written).write(row);
                output.add(written + 1).write(row + 1);
                output.add(written + 2).write(row + 2);
                output.add(written + 3).write(row + 3);
            }
            written += 4;
        } else {
            for (lane, &matched) in lanes.iter().enumerate() {
                if matched != 0 {
                    unsafe { output.add(written).write(row + lane) };
                    written += 1;
                }
            }
        }
        row += 4;
    }
    while row < rows {
        let value = i32::from_le(unsafe { input.add(row).read_unaligned() });
        if value <= upper_bound.value {
            unsafe { output.add(written).write(row) };
            written += 1;
        }
        row += 1;
    }
    unsafe { selection.set_len(start + written) };
    true
}

#[cfg(not(all(target_arch = "aarch64", target_endian = "little")))]
fn try_filter_seed_i32_batch(
    _batch: &PredicateColumnBatch,
    _kernel: &FixedConjunction<i32>,
    _rows: usize,
    _selection: &mut Vec<usize>,
    _seed: bool,
) -> bool {
    false
}

fn filter_fixed_batch<T: FixedPhysical>(
    batch: &PredicateColumnBatch,
    kernel: &FixedConjunction<T>,
    rows: usize,
    selection: &mut Vec<usize>,
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
    selection: &mut Vec<usize>,
    seed: bool,
    load: L,
    valid: V,
) where
    T: FixedPhysical,
    L: Fn(usize) -> T + Copy,
    V: Fn(usize) -> bool + Copy,
{
    if kernel.contradiction {
        selection.clear();
        return;
    }
    if let Some(expected) = kernel.equality {
        filter_selection(rows, selection, seed, load, valid, |value| {
            value == expected
        });
        return;
    }
    if let Some(inclusions) = &kernel.inclusions {
        match inclusions.view() {
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
        }
        return;
    }
    if !kernel.exclusions.is_empty() {
        filter_selection(rows, selection, seed, load, valid, |value| {
            kernel.matches(value)
        });
        return;
    }

    match (kernel.lower, kernel.upper) {
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
    }
}

#[inline]
fn filter_selection<T, L, V, P>(
    rows: usize,
    selection: &mut Vec<usize>,
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
                spare[written].write(row_idx);
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
        let row_idx = selection[read_idx];
        if valid(row_idx) && predicate(load(row_idx)) {
            selection[write_idx] = row_idx;
            write_idx += 1;
        }
    }
    selection.truncate(write_idx);
}
