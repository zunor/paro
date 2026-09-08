// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Plan-local distribution estimates, never pruning or execution proofs.

/// First two moments of a normal approximation in logical numeric units.
///
/// Private, finite bit encodings provide a stable value identity without NaN
/// equality surprises. These are estimates, not min/max bounds: neither zero
/// variance nor an extreme tail permits a semantic simplification. Storage
/// serialization intentionally omits this query-local evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EstimatedNumericDistribution {
    mean: u64,
    variance: u64,
}

impl EstimatedNumericDistribution {
    pub fn normal(mean: f64, variance: f64) -> Option<Self> {
        if !mean.is_finite() || !variance.is_finite() || variance < 0.0 {
            return None;
        }
        let canonical = |value: f64| if value == 0.0 { 0.0 } else { value }.to_bits();
        Some(Self {
            mean: canonical(mean),
            variance: canonical(variance),
        })
    }

    pub fn mean(self) -> f64 {
        f64::from_bits(self.mean)
    }

    pub fn variance(self) -> f64 {
        f64::from_bits(self.variance)
    }

    pub fn encoding(self) -> [u8; 16] {
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&self.mean.to_le_bytes());
        bytes[8..].copy_from_slice(&self.variance.to_le_bytes());
        bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distribution_identity_is_finite_and_canonical() {
        assert!(EstimatedNumericDistribution::normal(f64::NAN, 1.0).is_none());
        assert!(EstimatedNumericDistribution::normal(0.0, f64::INFINITY).is_none());
        assert!(EstimatedNumericDistribution::normal(0.0, -1.0).is_none());
        assert_eq!(
            EstimatedNumericDistribution::normal(-0.0, -0.0),
            EstimatedNumericDistribution::normal(0.0, 0.0)
        );
    }
}
