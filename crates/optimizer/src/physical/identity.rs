// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Stable, arena-local optimizer identifiers shared with extracted plans.

use std::fmt;

use sha2::{Digest, Sha256};

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
id_type!(CandidateId);
id_type!(LogicalPayloadId);
id_type!(PhysicalPayloadId);
id_type!(PropertySetId);
id_type!(OptimizationContextId);
id_type!(ResourceGrantClassId);
id_type!(AdmissibleGrantSetId);
id_type!(RuleId);
id_type!(ImplementationId);
id_type!(EnforcerRecipeId);
id_type!(RegionId);
id_type!(FactorizationSpecId);
id_type!(QualityPolicyId);
id_type!(BaseRelationId);
id_type!(SnapshotId);
id_type!(MutationBarrierId);
id_type!(StableReadProofId);
id_type!(LocatorKindId);
id_type!(CollationId);
id_type!(ExternalWorkerRequirementSetId);
id_type!(OpClassId);
id_type!(CalibrationRevisionId);

/// Stable structural identity. Construction must use [`StableFingerprintBuilder`]
/// rather than `DefaultHasher`, whose seed/algorithm is not a plan contract.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Fingerprint(pub u128);

impl fmt::Debug for Fingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

/// Deterministic, domain-delimited structural identity builder.
///
/// Logical operator fingerprints are themselves part of Memo equality, so a
/// collision cannot rely on a later structural comparison to repair it. Use a
/// cryptographic digest and retain 128 bits rather than composing correlated
/// non-cryptographic lanes.
#[derive(Debug, Clone)]
pub struct StableFingerprintBuilder {
    state: Sha256,
    transcript: Option<Vec<u8>>,
}

impl Default for StableFingerprintBuilder {
    fn default() -> Self {
        let mut state = Sha256::new();
        state.update(b"paro.stable-fingerprint.v2");
        Self {
            state,
            transcript: None,
        }
    }
}

impl StableFingerprintBuilder {
    const BYTES_TAG: u8 = 1;
    const U64_TAG: u8 = 2;
    const FINGERPRINT_TAG: u8 = 3;

    /// Record the exact domain-delimited byte stream alongside its hash.
    /// Memo operator interning uses this form so a digest collision only
    /// selects a bucket and can never establish logical equivalence.
    pub fn recording() -> Self {
        let mut builder = Self::default();
        builder.transcript = Some(b"paro.stable-fingerprint.v2".to_vec());
        builder
    }

    fn update(&mut self, bytes: &[u8]) {
        self.state.update(bytes);
        if let Some(transcript) = &mut self.transcript {
            transcript.extend_from_slice(bytes);
        }
    }

    pub fn write_bytes(&mut self, bytes: &[u8]) {
        self.update(&[Self::BYTES_TAG]);
        self.update(&(bytes.len() as u64).to_le_bytes());
        self.update(bytes);
    }

    pub fn write_u64(&mut self, value: u64) {
        self.update(&[Self::U64_TAG]);
        self.update(&value.to_le_bytes());
    }

    pub fn write_fingerprint(&mut self, value: Fingerprint) {
        self.update(&[Self::FINGERPRINT_TAG]);
        self.update(&value.0.to_le_bytes());
    }

    pub fn finish(self) -> Fingerprint {
        let digest = self.state.finalize();
        let mut bytes = [0_u8; 16];
        bytes.copy_from_slice(&digest[..16]);
        Fingerprint(u128::from_le_bytes(bytes))
    }

    pub fn finish_recording(self) -> (Fingerprint, Box<[u8]>) {
        let transcript = self
            .transcript
            .clone()
            .expect("finish_recording requires StableFingerprintBuilder::recording");
        (self.finish(), transcript.into_boxed_slice())
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

    #[test]
    fn fingerprint_fields_are_type_delimited() {
        let mut bytes = StableFingerprintBuilder::default();
        bytes.write_bytes(b"");
        let mut integer = StableFingerprintBuilder::default();
        integer.write_u64(0);
        let mut fingerprint = StableFingerprintBuilder::default();
        fingerprint.write_fingerprint(Fingerprint(0));

        let bytes = bytes.finish();
        let integer = integer.finish();
        let fingerprint = fingerprint.finish();
        assert_ne!(bytes, integer);
        assert_ne!(integer, fingerprint);
        assert_ne!(bytes, fingerprint);
    }
}
