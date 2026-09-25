// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Search-local identities. They never cross into an executable plan.
pub use paro_planner::physical::identity::*;
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

id_type!(ScalarExprId);
id_type!(GroupId);
id_type!(LogicalExprId);
id_type!(PhysicalExprId);
id_type!(CandidateId);
id_type!(LogicalPayloadId);
id_type!(PhysicalPayloadId);
id_type!(PropertySetId);
id_type!(OptimizationContextId);
id_type!(AdmissibleGrantSetId);
id_type!(RuleId);
id_type!(ImplementationId);
id_type!(RegionId);
