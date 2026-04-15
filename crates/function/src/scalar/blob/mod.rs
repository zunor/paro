// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! BLOB scalar functions

pub mod create_sort_key;
#[cfg(test)]
mod tests;

pub use create_sort_key::{
    encode_sort_key, encode_sort_key_into, get_create_sort_key_function, OrderModifiers,
};
