//! Stable, arena-local optimizer identifiers shared with extracted plans.

use std::fmt;

macro_rules! id_type {
    ($name:ident) => {
        #[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub u32);

        impl $name {
            pub const INVALID: Self = Self(u32::MAX);

            pub fn new(index: usize) -> Self {
                assert!(
                    index < u32::MAX as usize,
                    concat!(stringify!($name), " overflow")
                );
                Self(index as u32)
            }

            pub const fn index(self) -> usize {
                self.0 as usize
            }

            pub const fn is_valid(self) -> bool {
                self.0 != u32::MAX
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), self.0)
            }
        }
    };
}

id_type!(ColumnId);
id_type!(ScalarExprId);
id_type!(GroupId);
id_type!(LogicalExprId);
id_type!(PhysicalExprId);
id_type!(LogicalPayloadId);
id_type!(PhysicalPayloadId);
id_type!(PropertySetId);
id_type!(OptimizationContextId);
id_type!(ObjectiveProfileId);
id_type!(ResourceGrantClassId);
id_type!(AdmissibleGrantSetId);
id_type!(RuleId);
id_type!(ImplementationId);
id_type!(EnforcerRecipeId);
id_type!(RegionId);
id_type!(FactorizationSpecId);
id_type!(QualityPolicyId);
id_type!(UncertaintySetId);
id_type!(UncertaintySummaryId);
id_type!(ErrorFactorId);
id_type!(ProgressSummaryId);
id_type!(BaseRelationId);
id_type!(SnapshotId);
id_type!(MutationBarrierId);
id_type!(StableReadProofId);
id_type!(LocatorKindId);
id_type!(CollationId);
id_type!(ExternalWorkerRequirementSetId);
id_type!(ExternalWorkerPoolClassId);
id_type!(OpClassId);
id_type!(RoutineCostProfileId);
id_type!(CalibrationRevisionId);
id_type!(StatisticsSnapshotId);

/// Stable structural identity. Construction must use [`StableFingerprintBuilder`]
/// rather than `DefaultHasher`, whose seed/algorithm is not a plan contract.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Fingerprint(pub u128);

impl fmt::Debug for Fingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

/// Small deterministic FNV-1a based builder. This is an identity hash, not a
/// cryptographic digest; equality is always confirmed from canonical keys.
#[derive(Debug, Clone)]
pub struct StableFingerprintBuilder {
    high: u64,
    low: u64,
}

impl Default for StableFingerprintBuilder {
    fn default() -> Self {
        Self {
            high: 0xcbf29ce484222325,
            low: 0x84222325cbf29ce4,
        }
    }
}

impl StableFingerprintBuilder {
    const PRIME: u64 = 0x100000001b3;

    pub fn write_bytes(&mut self, bytes: &[u8]) {
        self.write_u64(bytes.len() as u64);
        for &byte in bytes {
            self.high ^= byte as u64;
            self.high = self.high.wrapping_mul(Self::PRIME);
            self.low ^= (byte as u64).rotate_left(1);
            self.low = self.low.wrapping_mul(Self::PRIME.rotate_left(7));
        }
    }

    pub fn write_u64(&mut self, value: u64) {
        for byte in value.to_le_bytes() {
            self.high ^= byte as u64;
            self.high = self.high.wrapping_mul(Self::PRIME);
            self.low ^= (byte as u64).rotate_left(1);
            self.low = self.low.wrapping_mul(Self::PRIME.rotate_left(7));
        }
    }

    pub fn write_fingerprint(&mut self, value: Fingerprint) {
        self.write_u64(value.0 as u64);
        self.write_u64((value.0 >> 64) as u64);
    }

    pub fn finish(self) -> Fingerprint {
        Fingerprint(((self.high as u128) << 64) | self.low as u128)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_is_stable_and_length_delimited() {
        let mut left = StableFingerprintBuilder::default();
        left.write_bytes(b"ab");
        left.write_bytes(b"c");

        let mut right = StableFingerprintBuilder::default();
        right.write_bytes(b"a");
        right.write_bytes(b"bc");

        assert_ne!(left.clone().finish(), right.finish());

        let mut repeat = StableFingerprintBuilder::default();
        repeat.write_bytes(b"ab");
        repeat.write_bytes(b"c");
        assert_eq!(left.finish(), repeat.finish());
    }
}
