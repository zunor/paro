// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Allocation-free row verification for binary-comparable varlen predicates.

use std::cell::RefCell;

use bytes::Bytes;
use paro_common::error::{self as paro_error, Result};

use crate::index::{FixedMembershipBuildPolicy, FixedMembershipSet};

use super::predicate_column::PredicateColumnBatch;
use super::segment_predicate::ComparisonOperator;

#[derive(Debug)]
struct VarlenBound {
    value: Box<[u8]>,
    inclusive: bool,
}

#[derive(Debug)]
struct DictionaryPredicateCache {
    dictionary: Bytes,
    matches: Box<[bool]>,
}

/// Normalized union of binary string prefixes.
///
/// Short equal-width domains use the fixed-width dense/sorted membership
/// representation; long equal-width domains use a binary search over borrowed
/// leading bytes. Mixed-width domains retain general `starts_with` semantics.
/// Redundant prefixes are removed once during compilation.
#[derive(Debug)]
pub(super) struct VarlenPrefixMembership {
    prefixes: Vec<Box<[u8]>>,
    uniform_width: Option<usize>,
    packed: Option<PackedPrefixMembership>,
    dictionary_cache: RefCell<Option<DictionaryPredicateCache>>,
}

#[derive(Debug)]
enum PackedPrefixMembership {
    I32(FixedMembershipSet<i32>),
    I64(FixedMembershipSet<i64>),
    I128(FixedMembershipSet<i128>),
}

impl PackedPrefixMembership {
    fn new(width: usize, prefixes: &[Box<[u8]>]) -> Self {
        let policy = FixedMembershipBuildPolicy::new(1 << 16, 256);
        match width {
            1..=4 => Self::I32(FixedMembershipSet::from_values_with_policy(
                prefixes
                    .iter()
                    .map(|prefix| pack_prefix(prefix) as i32)
                    .collect(),
                policy,
            )),
            5..=7 => Self::I64(FixedMembershipSet::from_values_with_policy(
                prefixes
                    .iter()
                    .map(|prefix| pack_prefix(prefix) as i64)
                    .collect(),
                policy,
            )),
            8 => Self::I128(FixedMembershipSet::from_values_with_policy(
                prefixes.iter().map(|prefix| pack_prefix(prefix)).collect(),
                policy,
            )),
            _ => unreachable!("packed prefix width is bounded by construction"),
        }
    }

    #[inline]
    fn contains(&self, prefix: &[u8]) -> bool {
        let packed = pack_prefix(prefix);
        match self {
            Self::I32(values) => values.contains(packed as i32),
            Self::I64(values) => values.contains(packed as i64),
            Self::I128(values) => values.contains(packed),
        }
    }
}

impl VarlenPrefixMembership {
    pub(super) fn new(prefixes: impl IntoIterator<Item = Box<[u8]>>) -> Self {
        let mut prefixes = prefixes.into_iter().collect::<Vec<_>>();
        prefixes.sort_unstable();
        prefixes.dedup();
        let mut normalized = Vec::<Box<[u8]>>::with_capacity(prefixes.len());
        for prefix in prefixes {
            if !normalized
                .iter()
                .any(|existing| prefix.starts_with(existing.as_ref()))
            {
                normalized.push(prefix);
            }
        }
        let uniform_width = normalized
            .first()
            .map(|prefix| prefix.len())
            .filter(|width| normalized.iter().all(|prefix| prefix.len() == *width));
        let packed = uniform_width
            .filter(|width| (1..=8).contains(width))
            .map(|width| PackedPrefixMembership::new(width, &normalized));
        Self {
            prefixes: normalized,
            uniform_width,
            packed,
            dictionary_cache: RefCell::new(None),
        }
    }

    #[inline]
    pub(super) fn matches(&self, value: &[u8]) -> bool {
        if let (Some(width), Some(packed)) = (self.uniform_width, &self.packed) {
            let Some(prefix) = value.get(..width) else {
                return false;
            };
            return packed.contains(prefix);
        }
        if let Some(width) = self.uniform_width {
            let Some(value_prefix) = value.get(..width) else {
                return false;
            };
            return self
                .prefixes
                .binary_search_by(|prefix| prefix.as_ref().cmp(value_prefix))
                .is_ok();
        }
        self.prefixes
            .iter()
            .any(|prefix| value.starts_with(prefix.as_ref()))
    }

    pub(super) fn evaluation_priority(&self) -> (u8, usize) {
        (2, self.prefixes.len())
    }

    pub(super) fn filter_batch(
        &self,
        batch: &PredicateColumnBatch,
        rows: usize,
        selection: &mut Vec<usize>,
        seed: bool,
    ) -> Result<()> {
        if let Some(batch) = batch.storage_dictionary() {
            let mut cache = self.dictionary_cache.borrow_mut();
            let dictionary = batch.encoded_dictionary();
            let cache_matches = cache.as_ref().is_some_and(|cache| {
                cache.dictionary.len() == dictionary.len()
                    && cache.dictionary.as_ptr() == dictionary.as_ptr()
            });
            if !cache_matches {
                let matches = (0..batch.dictionary_len())
                    .map(|code| self.matches(&batch.dictionary_value(code)))
                    .collect::<Vec<_>>()
                    .into_boxed_slice();
                *cache = Some(DictionaryPredicateCache {
                    dictionary: dictionary.clone(),
                    matches,
                });
            }
            batch.filter_codes(
                &cache
                    .as_ref()
                    .expect("dictionary cache initialized")
                    .matches,
                selection,
                seed,
            );
            return Ok(());
        }
        filter_varlen_batch(batch, rows, selection, seed, |value| self.matches(value))
    }
}

#[inline]
fn pack_prefix(prefix: &[u8]) -> i128 {
    prefix.iter().fold(0_i128, |packed, byte| {
        (packed << u8::BITS) | i128::from(*byte)
    })
}

/// Normalized conjunction over one binary-comparable varlen column.
///
/// Bounds and membership values are owned once at predicate compilation. Batch
/// evaluation compares borrowed vector bytes directly, so it never constructs
/// a row-level `Value` or `String`.
#[derive(Debug)]
pub(super) struct VarlenConjunction {
    equality: Option<Box<[u8]>>,
    lower: Option<VarlenBound>,
    upper: Option<VarlenBound>,
    inclusions: Option<Vec<Box<[u8]>>>,
    exclusions: Vec<Box<[u8]>>,
    required_prefix: Option<Box<[u8]>>,
    excluded_prefixes: Vec<Box<[u8]>>,
    contradiction: bool,
    dictionary_cache: RefCell<Option<DictionaryPredicateCache>>,
}

impl VarlenConjunction {
    pub(super) fn new(operator: ComparisonOperator, rhs: &[u8]) -> Self {
        let mut conjunction = Self::empty();
        conjunction.add(operator, rhs.into());
        conjunction
    }

    pub(super) fn from_in(values: impl IntoIterator<Item = Box<[u8]>>) -> Self {
        let mut values = values.into_iter().collect::<Vec<_>>();
        values.sort_unstable();
        values.dedup();
        let contradiction = values.is_empty();
        Self {
            inclusions: Some(values),
            contradiction,
            ..Self::empty()
        }
    }

    pub(super) fn from_range(lower: &[u8], upper: &[u8]) -> Self {
        let mut conjunction = Self::new(ComparisonOperator::GreaterThanOrEqual, lower);
        conjunction.add(ComparisonOperator::LessThanOrEqual, upper.into());
        conjunction
    }

    pub(super) fn from_prefix(prefix: &[u8], negated: bool) -> Self {
        let mut conjunction = Self::empty();
        conjunction.add_prefix(prefix.into(), negated);
        conjunction
    }

    fn empty() -> Self {
        Self {
            equality: None,
            lower: None,
            upper: None,
            inclusions: None,
            exclusions: Vec::new(),
            required_prefix: None,
            excluded_prefixes: Vec::new(),
            contradiction: false,
            dictionary_cache: RefCell::new(None),
        }
    }

    fn add_prefix(&mut self, prefix: Box<[u8]>, negated: bool) {
        self.dictionary_cache.get_mut().take();
        if negated {
            if prefix.is_empty() {
                self.contradiction = true;
            } else if !self
                .excluded_prefixes
                .iter()
                .any(|existing| prefix.starts_with(existing))
            {
                self.excluded_prefixes
                    .retain(|existing| !existing.starts_with(&prefix));
                self.excluded_prefixes.push(prefix);
            }
        } else {
            match self.required_prefix.as_deref() {
                Some(existing) if existing.starts_with(&prefix) => {}
                Some(existing) if prefix.starts_with(existing) => {
                    self.required_prefix = Some(prefix)
                }
                Some(_) => self.contradiction = true,
                None => self.required_prefix = Some(prefix),
            }
        }
        self.validate();
    }

    fn add(&mut self, operator: ComparisonOperator, rhs: Box<[u8]>) {
        self.dictionary_cache.get_mut().take();
        match operator {
            ComparisonOperator::Equal => match self.equality.as_deref() {
                Some(existing) if existing != rhs.as_ref() => self.contradiction = true,
                Some(_) => {}
                None => self.equality = Some(rhs),
            },
            ComparisonOperator::NotEqual => {
                if !self
                    .exclusions
                    .iter()
                    .any(|value| value.as_ref() == rhs.as_ref())
                {
                    self.exclusions.push(rhs);
                }
            }
            ComparisonOperator::LessThan | ComparisonOperator::LessThanOrEqual => {
                Self::tighten_upper(
                    &mut self.upper,
                    VarlenBound {
                        value: rhs,
                        inclusive: matches!(operator, ComparisonOperator::LessThanOrEqual),
                    },
                );
            }
            ComparisonOperator::GreaterThan | ComparisonOperator::GreaterThanOrEqual => {
                Self::tighten_lower(
                    &mut self.lower,
                    VarlenBound {
                        value: rhs,
                        inclusive: matches!(operator, ComparisonOperator::GreaterThanOrEqual),
                    },
                );
            }
        }
        self.validate();
    }

    pub(super) fn merge(&mut self, mut incoming: Self) {
        self.dictionary_cache.get_mut().take();
        if incoming.contradiction {
            self.contradiction = true;
        }
        if let Some(value) = incoming.equality.take() {
            self.add(ComparisonOperator::Equal, value);
        }
        if let Some(bound) = incoming.lower.take() {
            self.add(
                if bound.inclusive {
                    ComparisonOperator::GreaterThanOrEqual
                } else {
                    ComparisonOperator::GreaterThan
                },
                bound.value,
            );
        }
        if let Some(bound) = incoming.upper.take() {
            self.add(
                if bound.inclusive {
                    ComparisonOperator::LessThanOrEqual
                } else {
                    ComparisonOperator::LessThan
                },
                bound.value,
            );
        }
        if let Some(incoming_values) = incoming.inclusions.take() {
            if let Some(values) = &mut self.inclusions {
                values.retain(|value| incoming_values.binary_search(value).is_ok());
            } else {
                self.inclusions = Some(incoming_values);
            }
        }
        for value in incoming.exclusions.drain(..) {
            self.add(ComparisonOperator::NotEqual, value);
        }
        if let Some(prefix) = incoming.required_prefix.take() {
            self.add_prefix(prefix, false);
        }
        for prefix in incoming.excluded_prefixes.drain(..) {
            self.add_prefix(prefix, true);
        }
        self.validate();
    }

    fn tighten_lower(current: &mut Option<VarlenBound>, incoming: VarlenBound) {
        if current.as_ref().is_none_or(|existing| {
            incoming.value > existing.value
                || (incoming.value == existing.value && !incoming.inclusive && existing.inclusive)
        }) {
            *current = Some(incoming);
        }
    }

    fn tighten_upper(current: &mut Option<VarlenBound>, incoming: VarlenBound) {
        if current.as_ref().is_none_or(|existing| {
            incoming.value < existing.value
                || (incoming.value == existing.value && !incoming.inclusive && existing.inclusive)
        }) {
            *current = Some(incoming);
        }
    }

    fn validate(&mut self) {
        if let (Some(lower), Some(upper)) = (&self.lower, &self.upper) {
            self.contradiction |= lower.value > upper.value
                || (lower.value == upper.value && (!lower.inclusive || !upper.inclusive));
        }
        if let Some(value) = self.equality.as_deref() {
            self.contradiction |= self
                .exclusions
                .iter()
                .any(|excluded| excluded.as_ref() == value)
                || self.inclusions.as_ref().is_some_and(|values| {
                    values
                        .binary_search_by(|item| item.as_ref().cmp(value))
                        .is_err()
                })
                || self.lower.as_ref().is_some_and(|bound| {
                    value < bound.value.as_ref()
                        || (value == bound.value.as_ref() && !bound.inclusive)
                })
                || self.upper.as_ref().is_some_and(|bound| {
                    value > bound.value.as_ref()
                        || (value == bound.value.as_ref() && !bound.inclusive)
                })
                || self
                    .required_prefix
                    .as_deref()
                    .is_some_and(|prefix| !value.starts_with(prefix))
                || self
                    .excluded_prefixes
                    .iter()
                    .any(|prefix| value.starts_with(prefix));
        }
        self.contradiction |= self.required_prefix.as_deref().is_some_and(|required| {
            self.excluded_prefixes
                .iter()
                .any(|excluded| required.starts_with(excluded.as_ref()))
        });
        if let Some(values) = &mut self.inclusions {
            let equality = self.equality.as_deref();
            let lower = self.lower.as_ref();
            let upper = self.upper.as_ref();
            let exclusions = &self.exclusions;
            let required_prefix = self.required_prefix.as_deref();
            let excluded_prefixes = &self.excluded_prefixes;
            values.retain(|value| {
                let value = value.as_ref();
                equality.is_none_or(|expected| value == expected)
                    && lower.is_none_or(|bound| {
                        value > bound.value.as_ref()
                            || (bound.inclusive && value == bound.value.as_ref())
                    })
                    && upper.is_none_or(|bound| {
                        value < bound.value.as_ref()
                            || (bound.inclusive && value == bound.value.as_ref())
                    })
                    && !exclusions.iter().any(|excluded| excluded.as_ref() == value)
                    && required_prefix.is_none_or(|prefix| value.starts_with(prefix))
                    && !excluded_prefixes
                        .iter()
                        .any(|prefix| value.starts_with(prefix.as_ref()))
            });
            self.contradiction |= values.is_empty();
        }
    }

    #[inline]
    pub(super) fn matches(&self, value: &[u8]) -> bool {
        if self.contradiction {
            return false;
        }
        // Validation folds every other constraint into exact equality and IN
        // domains, so these canonical forms need only one comparison here.
        if let Some(expected) = self.equality.as_deref() {
            return value == expected;
        }
        if let Some(values) = &self.inclusions {
            return values
                .binary_search_by(|candidate| candidate.as_ref().cmp(value))
                .is_ok();
        }
        self.lower.as_ref().is_none_or(|bound| {
            value > bound.value.as_ref() || (bound.inclusive && value == bound.value.as_ref())
        }) && self.upper.as_ref().is_none_or(|bound| {
            value < bound.value.as_ref() || (bound.inclusive && value == bound.value.as_ref())
        }) && !self
            .exclusions
            .iter()
            .any(|excluded| excluded.as_ref() == value)
            && self
                .required_prefix
                .as_deref()
                .is_none_or(|prefix| value.starts_with(prefix))
            && !self
                .excluded_prefixes
                .iter()
                .any(|prefix| value.starts_with(prefix.as_ref()))
    }

    /// Static fallback ordering used when the storage layer has no histogram
    /// estimate for this predicate.
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
        if self.required_prefix.is_some() || (self.lower.is_some() && self.upper.is_some()) {
            return (3, 0);
        }
        if self.lower.is_some() || self.upper.is_some() {
            return (4, 0);
        }
        if !self.exclusions.is_empty() || !self.excluded_prefixes.is_empty() {
            return (6, self.exclusions.len() + self.excluded_prefixes.len());
        }
        (7, 0)
    }

    pub(super) fn filter_batch(
        &self,
        batch: &PredicateColumnBatch,
        rows: usize,
        selection: &mut Vec<usize>,
        seed: bool,
    ) -> Result<()> {
        if let Some(batch) = batch.storage_dictionary() {
            let mut cache = self.dictionary_cache.borrow_mut();
            let dictionary = batch.encoded_dictionary();
            let cache_matches = cache.as_ref().is_some_and(|cache| {
                cache.dictionary.len() == dictionary.len()
                    && cache.dictionary.as_ptr() == dictionary.as_ptr()
            });
            if !cache_matches {
                let matches = (0..batch.dictionary_len())
                    .map(|code| self.matches(&batch.dictionary_value(code)))
                    .collect::<Vec<_>>()
                    .into_boxed_slice();
                *cache = Some(DictionaryPredicateCache {
                    dictionary: dictionary.clone(),
                    matches,
                });
            }
            batch.filter_codes(
                &cache
                    .as_ref()
                    .expect("dictionary cache initialized")
                    .matches,
                selection,
                seed,
            );
            return Ok(());
        }
        if self.contradiction {
            selection.clear();
            return Ok(());
        }
        if let Some(expected) = self.equality.as_deref() {
            return filter_varlen_batch(batch, rows, selection, seed, |value| value == expected);
        }
        if let Some(values) = &self.inclusions {
            return filter_varlen_batch(batch, rows, selection, seed, |value| {
                values
                    .binary_search_by(|candidate| candidate.as_ref().cmp(value))
                    .is_ok()
            });
        }
        if self.lower.is_none()
            && self.upper.is_none()
            && self.required_prefix.is_none()
            && self.excluded_prefixes.is_empty()
        {
            return filter_varlen_batch(batch, rows, selection, seed, |value| {
                !self
                    .exclusions
                    .iter()
                    .any(|excluded| excluded.as_ref() == value)
            });
        }
        if self.lower.is_none()
            && self.upper.is_none()
            && self.exclusions.is_empty()
            && self.required_prefix.is_none()
        {
            return filter_varlen_batch(batch, rows, selection, seed, |value| {
                !self
                    .excluded_prefixes
                    .iter()
                    .any(|prefix| value.starts_with(prefix.as_ref()))
            });
        }
        filter_varlen_batch(batch, rows, selection, seed, |value| self.matches(value))
    }
}

fn filter_varlen_batch(
    batch: &PredicateColumnBatch,
    rows: usize,
    selection: &mut Vec<usize>,
    seed: bool,
    matches: impl Fn(&[u8]) -> bool,
) -> Result<()> {
    if let Some(batch) = batch.raw_varlen() {
        let row_matches = |row_idx: usize| batch.row_value(row_idx).is_some_and(&matches);
        if seed {
            selection.extend((0..rows).filter(|row_idx| row_matches(*row_idx)));
        } else {
            selection.retain(|row_idx| row_matches(*row_idx));
        }
        return Ok(());
    }
    let vector = batch
        .decoded()
        .ok_or_else(|| paro_error::internal("varlen predicate received a fixed-width batch"))?;
    let view = vector.try_to_varlen_view(rows)?;
    let row_matches = |row_idx: usize| {
        view.is_valid(row_idx) && matches(view.get_inline_string(row_idx).as_bytes())
    };
    if seed {
        selection.extend((0..rows).filter(|row_idx| row_matches(*row_idx)));
    } else {
        selection.retain(|row_idx| row_matches(*row_idx));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conjunction_normalizes_bounds_membership_and_exclusions() {
        let mut values = VarlenConjunction::from_in(
            [
                b"a".as_slice(),
                b"b".as_slice(),
                b"c".as_slice(),
                b"d".as_slice(),
            ]
            .into_iter()
            .map(Box::<[u8]>::from),
        );
        values.merge(VarlenConjunction::new(
            ComparisonOperator::GreaterThan,
            b"a",
        ));
        values.merge(VarlenConjunction::new(ComparisonOperator::NotEqual, b"c"));

        assert!(!values.matches(b"a"));
        assert!(values.matches(b"b"));
        assert!(!values.matches(b"c"));
        assert!(values.matches(b"d"));
    }

    #[test]
    fn conjunction_normalizes_required_and_excluded_prefixes() {
        let mut values = VarlenConjunction::from_prefix(b"MEDIUM", false);
        values.merge(VarlenConjunction::from_prefix(b"MEDIUM POLISHED", true));

        assert!(values.matches(b"MEDIUM BRUSHED"));
        assert!(!values.matches(b"MEDIUM POLISHED COPPER"));
        assert!(!values.matches(b"LARGE BRUSHED"));

        values.merge(VarlenConjunction::from_prefix(b"MEDIUM POLISHED", false));
        assert!(!values.matches(b"MEDIUM POLISHED COPPER"));
    }

    #[test]
    fn short_uniform_prefixes_compile_to_fixed_membership() {
        let prefixes = VarlenPrefixMembership::new(
            [b"13".as_slice(), b"31".as_slice(), b"18".as_slice()]
                .into_iter()
                .map(Box::<[u8]>::from),
        );

        assert!(matches!(
            prefixes.packed,
            Some(PackedPrefixMembership::I32(_))
        ));
        assert!(prefixes.matches(b"13-555-1234"));
        assert!(prefixes.matches(b"31"));
        assert!(!prefixes.matches(b"3"));
        assert!(!prefixes.matches(b"99-555-1234"));
    }
}
