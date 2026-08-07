// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Immutable physical-value sets for fixed-width storage predicates.
//!
//! Dense domains use a bitset and sparse domains use sorted values. The
//! representation is reference counted because runtime predicates are cloned
//! into independent segment readers after a join build completes.

use std::sync::Arc;

/// Bound dense membership so an adversarial sparse range cannot reserve memory
/// proportional to its endpoints.
const MAX_DENSE_MEMBERSHIP_BITS: usize = 1 << 20;
const MAX_DENSE_MEMBERSHIP_BITS_PER_VALUE: usize = 16;

pub(crate) trait FixedMembershipValue: Copy + Ord {
    fn offset_from(self, base: Self) -> Option<usize>;
    fn checked_add_offset(self, offset: usize) -> Option<Self>;
}

macro_rules! impl_fixed_membership_value {
    ($ty:ty, $wide:ty) => {
        impl FixedMembershipValue for $ty {
            #[inline]
            fn offset_from(self, base: Self) -> Option<usize> {
                let offset = <$wide>::from(self).checked_sub(<$wide>::from(base))?;
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

impl_fixed_membership_value!(i32, i64);
impl_fixed_membership_value!(i64, i128);

impl FixedMembershipValue for i128 {
    #[inline]
    fn offset_from(self, base: Self) -> Option<usize> {
        usize::try_from(self.checked_sub(base)?).ok()
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

/// Immutable membership set for one physical integer width.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FixedMembershipSet<T> {
    representation: FixedMembershipRepresentation<T>,
}

impl<T: FixedMembershipValue> FixedMembershipSet<T> {
    pub(crate) fn from_values(mut values: Vec<T>) -> Self {
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
            .filter(|span| {
                *span <= MAX_DENSE_MEMBERSHIP_BITS
                    && *span
                        <= values
                            .len()
                            .saturating_mul(MAX_DENSE_MEMBERSHIP_BITS_PER_VALUE)
            })
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

    pub(crate) fn len(&self) -> usize {
        match &self.representation {
            FixedMembershipRepresentation::Sorted(values) => values.len(),
            FixedMembershipRepresentation::Dense { count, .. } => *count,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
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

    pub fn len(&self) -> usize {
        match &self.kind {
            FixedMembershipKind::I32(values) => values.len(),
            FixedMembershipKind::I64(values) => values.len(),
            FixedMembershipKind::I128(values) => values.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
}
