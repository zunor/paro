// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Immutable physical-value sets for fixed-width storage predicates.
//!
//! Dense domains use a bitset and sparse domains use sorted values. The
//! representation is reference counted because runtime predicates are cloned
//! into independent segment readers after a join build completes.

use std::sync::Arc;

/// Construction limits for the dense representation of a fixed-width set.
///
/// Callers with different access patterns can choose a policy without changing
/// the set's lookup contract. Static predicates use the conservative default;
/// analytical runtime filters may spend more bounded memory to avoid a binary
/// search for every scanned row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FixedMembershipBuildPolicy {
    max_dense_bits: usize,
    max_dense_bits_per_value: usize,
}

impl FixedMembershipBuildPolicy {
    pub const fn new(max_dense_bits: usize, max_dense_bits_per_value: usize) -> Self {
        Self {
            max_dense_bits,
            max_dense_bits_per_value,
        }
    }

    fn permits_dense(self, span: usize, value_count: usize) -> bool {
        span <= self.max_dense_bits
            && span <= value_count.saturating_mul(self.max_dense_bits_per_value)
    }
}

impl Default for FixedMembershipBuildPolicy {
    fn default() -> Self {
        Self::new(1 << 20, 16)
    }
}

pub(crate) trait FixedMembershipValue: Copy + Ord {
    fn offset_from(self, base: Self) -> Option<usize>;
    fn checked_add_offset(self, offset: usize) -> Option<Self>;
}

macro_rules! impl_fixed_membership_value {
    ($ty:ty, $unsigned:ty, $wide:ty) => {
        impl FixedMembershipValue for $ty {
            #[inline]
            fn offset_from(self, base: Self) -> Option<usize> {
                if self < base {
                    return None;
                }
                // With ordered signed endpoints, wrapping subtraction in the
                // corresponding unsigned domain is exactly their mathematical
                // non-negative distance, including ranges crossing zero.
                let offset = (self as $unsigned).wrapping_sub(base as $unsigned);
                usize::try_from(offset).ok()
            }

            #[inline]
            fn checked_add_offset(self, offset: usize) -> Option<Self> {
                let value = <$wide>::from(self).checked_add(<$wide>::try_from(offset).ok()?)?;
                Self::try_from(value).ok()
            }
        }
    };
}

impl_fixed_membership_value!(i32, u32, i64);
impl_fixed_membership_value!(i64, u64, i128);

impl FixedMembershipValue for i128 {
    #[inline]
    fn offset_from(self, base: Self) -> Option<usize> {
        if self < base {
            return None;
        }
        usize::try_from((self as u128).wrapping_sub(base as u128)).ok()
    }

    #[inline]
    fn checked_add_offset(self, offset: usize) -> Option<Self> {
        self.checked_add(i128::try_from(offset).ok()?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FixedMembershipRepresentation<T> {
    Sorted(Arc<[T]>),
    Dense {
        base: T,
        span: usize,
        count: usize,
        bits: Arc<[u64]>,
    },
}

/// Borrowed physical representation used to dispatch vector kernels once per
/// batch instead of once per value.
pub(crate) enum FixedMembershipView<'a, T> {
    Sorted(&'a [T]),
    Dense {
        base: T,
        span: usize,
        bits: &'a [u64],
    },
}

/// Immutable membership set for one physical integer width.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FixedMembershipSet<T> {
    representation: FixedMembershipRepresentation<T>,
}

impl<T: FixedMembershipValue> FixedMembershipSet<T> {
    pub(crate) fn from_values(values: Vec<T>) -> Self {
        Self::from_values_with_policy(values, FixedMembershipBuildPolicy::default())
    }

    fn from_values_with_policy(mut values: Vec<T>, policy: FixedMembershipBuildPolicy) -> Self {
        if values.is_empty() {
            return Self {
                representation: FixedMembershipRepresentation::Sorted(Arc::from([])),
            };
        }

        let mut min = values[0];
        let mut max = values[0];
        for &value in &values[1..] {
            min = min.min(value);
            max = max.max(value);
        }
        if let Some(span) = max
            .offset_from(min)
            .and_then(|offset| offset.checked_add(1))
            .filter(|span| policy.permits_dense(*span, values.len()))
        {
            let mut bits = vec![0_u64; span.div_ceil(u64::BITS as usize)];
            let mut count = 0usize;
            for value in values {
                let offset = value
                    .offset_from(min)
                    .expect("membership value lies inside measured dense range");
                let word = &mut bits[offset / u64::BITS as usize];
                let mask = 1_u64 << (offset % u64::BITS as usize);
                if *word & mask == 0 {
                    *word |= mask;
                    count += 1;
                }
            }
            return Self {
                representation: FixedMembershipRepresentation::Dense {
                    base: min,
                    span,
                    count,
                    bits: bits.into(),
                },
            };
        }

        values.sort_unstable();
        values.dedup();
        Self {
            representation: FixedMembershipRepresentation::Sorted(values.into()),
        }
    }

    #[inline]
    pub(crate) fn contains(&self, value: T) -> bool {
        match &self.representation {
            FixedMembershipRepresentation::Sorted(values) => values.binary_search(&value).is_ok(),
            FixedMembershipRepresentation::Dense {
                base, span, bits, ..
            } => {
                let Some(offset) = value.offset_from(*base).filter(|offset| *offset < *span) else {
                    return false;
                };
                bits[offset / u64::BITS as usize] & (1_u64 << (offset % u64::BITS as usize)) != 0
            }
        }
    }

    pub(crate) fn view(&self) -> FixedMembershipView<'_, T> {
        match &self.representation {
            FixedMembershipRepresentation::Sorted(values) => FixedMembershipView::Sorted(values),
            FixedMembershipRepresentation::Dense {
                base, span, bits, ..
            } => FixedMembershipView::Dense {
                base: *base,
                span: *span,
                bits,
            },
        }
    }

    pub(crate) fn len(&self) -> usize {
        match &self.representation {
            FixedMembershipRepresentation::Sorted(values) => values.len(),
            FixedMembershipRepresentation::Dense { count, .. } => *count,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn allocation_size(&self) -> usize {
        match &self.representation {
            FixedMembershipRepresentation::Sorted(values) => {
                values.len().saturating_mul(std::mem::size_of::<T>())
            }
            FixedMembershipRepresentation::Dense { bits, .. } => {
                bits.len().saturating_mul(std::mem::size_of::<u64>())
            }
        }
    }

    pub(crate) fn is_contiguous(&self) -> bool {
        match &self.representation {
            FixedMembershipRepresentation::Sorted(values) => {
                let (Some(first), Some(last)) = (values.first(), values.last()) else {
                    return false;
                };
                last.offset_from(*first)
                    .and_then(|offset| offset.checked_add(1))
                    == Some(values.len())
            }
            FixedMembershipRepresentation::Dense { span, count, .. } => span == count,
        }
    }

    pub(crate) fn first(&self) -> Option<T> {
        match &self.representation {
            FixedMembershipRepresentation::Sorted(values) => values.first().copied(),
            FixedMembershipRepresentation::Dense { base, bits, .. } => {
                let word_idx = bits.iter().position(|word| *word != 0)?;
                let bit_idx = bits[word_idx].trailing_zeros() as usize;
                base.checked_add_offset(word_idx * u64::BITS as usize + bit_idx)
            }
        }
    }

    pub(crate) fn last(&self) -> Option<T> {
        match &self.representation {
            FixedMembershipRepresentation::Sorted(values) => values.last().copied(),
            FixedMembershipRepresentation::Dense {
                base, span, bits, ..
            } => {
                let word_idx = bits.iter().rposition(|word| *word != 0)?;
                let bit_idx = (u64::BITS - 1 - bits[word_idx].leading_zeros()) as usize;
                let offset = word_idx * u64::BITS as usize + bit_idx;
                (offset < *span)
                    .then(|| base.checked_add_offset(offset))
                    .flatten()
            }
        }
    }

    pub(crate) fn retain(&mut self, mut predicate: impl FnMut(T) -> bool) {
        let values = self
            .iter()
            .filter(|value| predicate(*value))
            .collect::<Vec<_>>();
        *self = Self::from_values(values);
    }

    pub(crate) fn intersect(&mut self, other: &Self) {
        self.retain(|value| other.contains(value));
    }

    fn iter(&self) -> FixedMembershipIter<'_, T> {
        FixedMembershipIter {
            set: self,
            next_offset: 0,
        }
    }
}

struct FixedMembershipIter<'a, T> {
    set: &'a FixedMembershipSet<T>,
    next_offset: usize,
}

impl<T: FixedMembershipValue> Iterator for FixedMembershipIter<'_, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match &self.set.representation {
            FixedMembershipRepresentation::Sorted(values) => {
                let value = values.get(self.next_offset).copied();
                self.next_offset += usize::from(value.is_some());
                value
            }
            FixedMembershipRepresentation::Dense {
                base, span, bits, ..
            } => {
                while self.next_offset < *span {
                    let offset = self.next_offset;
                    self.next_offset += 1;
                    if bits[offset / u64::BITS as usize] & (1_u64 << (offset % u64::BITS as usize))
                        != 0
                    {
                        return base.checked_add_offset(offset);
                    }
                }
                None
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FixedMembershipKind {
    I32(FixedMembershipSet<i32>),
    I64(FixedMembershipSet<i64>),
    I128(FixedMembershipSet<i128>),
}

/// Type-erased fixed-width membership used by [`super::Predicate`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixedMembership {
    kind: FixedMembershipKind,
}

impl FixedMembership {
    pub fn i32(values: Vec<i32>) -> Self {
        Self {
            kind: FixedMembershipKind::I32(FixedMembershipSet::from_values(values)),
        }
    }

    pub fn i64(values: Vec<i64>) -> Self {
        Self {
            kind: FixedMembershipKind::I64(FixedMembershipSet::from_values(values)),
        }
    }

    pub fn i128(values: Vec<i128>) -> Self {
        Self {
            kind: FixedMembershipKind::I128(FixedMembershipSet::from_values(values)),
        }
    }

    pub fn i32_with_policy(values: Vec<i32>, policy: FixedMembershipBuildPolicy) -> Self {
        Self {
            kind: FixedMembershipKind::I32(FixedMembershipSet::from_values_with_policy(
                values, policy,
            )),
        }
    }

    pub fn i64_with_policy(values: Vec<i64>, policy: FixedMembershipBuildPolicy) -> Self {
        Self {
            kind: FixedMembershipKind::I64(FixedMembershipSet::from_values_with_policy(
                values, policy,
            )),
        }
    }

    pub fn i128_with_policy(values: Vec<i128>, policy: FixedMembershipBuildPolicy) -> Self {
        Self {
            kind: FixedMembershipKind::I128(FixedMembershipSet::from_values_with_policy(
                values, policy,
            )),
        }
    }

    pub fn len(&self) -> usize {
        match &self.kind {
            FixedMembershipKind::I32(values) => values.len(),
            FixedMembershipKind::I64(values) => values.len(),
            FixedMembershipKind::I128(values) => values.len(),
        }
    }

    /// Heap bytes retained by the immutable membership representation.
    pub fn allocation_size(&self) -> usize {
        match &self.kind {
            FixedMembershipKind::I32(values) => values.allocation_size(),
            FixedMembershipKind::I64(values) => values.allocation_size(),
            FixedMembershipKind::I128(values) => values.allocation_size(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_contiguous(&self) -> bool {
        match &self.kind {
            FixedMembershipKind::I32(values) => values.is_contiguous(),
            FixedMembershipKind::I64(values) => values.is_contiguous(),
            FixedMembershipKind::I128(values) => values.is_contiguous(),
        }
    }

    pub(crate) fn into_kind(self) -> FixedMembershipKind {
        self.kind
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dense_membership_deduplicates_and_iterates_in_order() {
        let mut values = FixedMembershipSet::from_values(vec![12_i32, 10, 12, 15]);
        assert_eq!(values.len(), 3);
        assert_eq!(values.first(), Some(10));
        assert_eq!(values.last(), Some(15));
        assert!(values.contains(12));
        assert!(!values.contains(11));
        assert_eq!(values.iter().collect::<Vec<_>>(), vec![10, 12, 15]);

        values.retain(|value| value >= 12);
        assert_eq!(values.iter().collect::<Vec<_>>(), vec![12, 15]);
    }

    #[test]
    fn sparse_membership_uses_the_same_set_contract() {
        let mut values = FixedMembershipSet::from_values(vec![0_i64, 1_000_000_000, -5]);
        assert_eq!(values.first(), Some(-5));
        assert_eq!(values.last(), Some(1_000_000_000));
        assert!(values.contains(0));
        values.intersect(&FixedMembershipSet::from_values(vec![-5, 7]));
        assert_eq!(values.iter().collect::<Vec<_>>(), vec![-5]);
    }

    #[test]
    fn physical_offsets_are_exact_across_signed_boundaries() {
        assert_eq!(5_i32.offset_from(-5), Some(10));
        assert_eq!((-5_i32).offset_from(5), None);
        assert_eq!(i32::MAX.offset_from(i32::MIN), Some(u32::MAX as usize));
        assert_eq!(5_i64.offset_from(-5), Some(10));
        assert_eq!((-5_i64).offset_from(5), None);
        assert_eq!(5_i128.offset_from(-5), Some(10));
    }

    #[test]
    fn construction_policy_selects_representation_without_changing_contract() {
        let source = vec![0_i64, 128, 256];
        let conservative = FixedMembershipSet::from_values(source.clone());
        let analytical = FixedMembershipSet::from_values_with_policy(
            source,
            FixedMembershipBuildPolicy::new(512, 256),
        );

        assert!(matches!(
            conservative.representation,
            FixedMembershipRepresentation::Sorted(_)
        ));
        assert!(matches!(
            analytical.representation,
            FixedMembershipRepresentation::Dense { .. }
        ));
        assert_eq!(
            conservative.iter().collect::<Vec<_>>(),
            analytical.iter().collect::<Vec<_>>()
        );
    }
}
