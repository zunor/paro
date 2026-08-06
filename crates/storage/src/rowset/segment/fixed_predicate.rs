// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::segment_predicate::{ComparisonOperator, PredicateColumnBatch};

#[derive(Clone, Copy)]
pub(super) struct FixedBound<T> {
    pub(super) value: T,
    pub(super) inclusive: bool,
}

pub(super) struct FixedConjunction<T> {
    pub(super) equality: Option<T>,
    pub(super) lower: Option<FixedBound<T>>,
    pub(super) upper: Option<FixedBound<T>>,
    exclusions: Vec<T>,
    contradiction: bool,
}

trait FixedPhysical: Copy + Ord {
    fn from_le(value: Self) -> Self;
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

impl<T: Copy + Ord> FixedConjunction<T> {
    pub(super) fn new(operator: ComparisonOperator, rhs: T) -> Self {
        let mut conjunction = Self {
            equality: None,
            lower: None,
            upper: None,
            exclusions: Vec::new(),
            contradiction: false,
        };
        conjunction.add(operator, rhs);
        conjunction
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
        for value in other.exclusions.drain(..) {
            self.add(ComparisonOperator::NotEqual, value);
        }
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
                || self.lower.is_some_and(|bound| {
                    value < bound.value || (value == bound.value && !bound.inclusive)
                })
                || self.upper.is_some_and(|bound| {
                    value > bound.value || (value == bound.value && !bound.inclusive)
                });
        }
    }

    #[inline]
    pub(super) fn matches(&self, value: T) -> bool {
        !self.contradiction
            && self.equality.is_none_or(|expected| value == expected)
            && self.lower.is_none_or(|bound| {
                value > bound.value || (bound.inclusive && value == bound.value)
            })
            && self.upper.is_none_or(|bound| {
                value < bound.value || (bound.inclusive && value == bound.value)
            })
            && !self.exclusions.contains(&value)
    }
}

pub(super) enum FixedComparisonValues {
    I32(FixedConjunction<i32>),
    I64(FixedConjunction<i64>),
    I128(FixedConjunction<i128>),
}

impl FixedComparisonValues {
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
    ) {
        match self {
            Self::I32(kernel) => filter_fixed_batch(batch, kernel, rows, selection, seed),
            Self::I64(kernel) => filter_fixed_batch(batch, kernel, rows, selection, seed),
            Self::I128(kernel) => filter_fixed_batch(batch, kernel, rows, selection, seed),
        }
    }
}

fn filter_fixed_batch<T: FixedPhysical>(
    batch: &PredicateColumnBatch,
    kernel: &FixedConjunction<T>,
    rows: usize,
    selection: &mut Vec<usize>,
    seed: bool,
) {
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
        }
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
    T: Copy + Ord,
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
        for row_idx in 0..rows {
            if valid(row_idx) && predicate(load(row_idx)) {
                selection.push(row_idx);
            }
        }
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
