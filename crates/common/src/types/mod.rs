// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Type Hierarchy
//! - Primitive: Boolean, Integer, Float, etc.
//! - Compound: Array (fixed-size), List (variable-size), Struct

mod logical_type;
mod nested_types;
pub mod pg_oid;
pub mod pg_type_descriptor;
mod physical_type;
mod string_view;

pub use logical_type::LogicalType;
pub use nested_types::{ArrayType, ListType, StructType};
pub use pg_type_descriptor::{logical_type_from_pg_oid, PgTypeDescriptor};
pub use physical_type::PhysicalType;
pub use string_view::{StringView, INLINE_CAPACITY, PREFIX_LEN};
