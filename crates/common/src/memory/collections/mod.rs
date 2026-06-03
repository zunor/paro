// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Grant-accounted standard collection wrappers.

mod bytes;
mod hash_map;
mod hash_set;
mod string;
mod vec;

pub use bytes::AccountedBytesMut;
pub use hash_map::AccountedHashMap;
pub use hash_set::{AccountedHashSet, PrecomputedHashBuildHasher};
pub use string::AccountedString;
pub use vec::AccountedVec;

#[inline]
fn bytes_for_capacity<T>(capacity: usize) -> usize {
    capacity.saturating_mul(std::mem::size_of::<T>())
}
